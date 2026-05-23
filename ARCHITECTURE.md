# DeepTutor 系统架构文档 (v7.0+)

> 本文档基于当前代码实现（v7.0+），反映真实运行时的系统架构、组件关系和核心流程。

---

## 架构原则

1. **本地 LLM 优先，云端 LLM 兜底** — 每次教学调用优先尝试本地 NPU（rkllama），若 NPU 被 OCR 占用，平台透明切换 DT 到云端 DeepSeek API，用户无感知。

2. **Hermes Agent 为中枢，DT 为教学工具** — Hermes Agent (HA) 是系统中枢，接收所有微信消息并路由到对应后端（教学 → platform → DT，闲聊 → LLM）。DT 是 HA 调用的教学引擎，无独立用户入口。

3. **微信为首要入口，DT Web UI 为补充** — 日常学习通过微信进行（拍作业照片、获取引导、即时反馈）。Web UI (3782) 支持深度学习（集中复习、强化练习）。

4. **微信会话必须保证实时性** — iLink 长轮询约 5 分钟过期，所有后台处理（OCR、LLM 推理、教学生成）必须在窗口内完成。

5. **DT 与 HA 的 LLM 资源竞争必须最优化** — 两者共用本地 NPU，全局 `asyncio.Lock` 序列化访问，差异化超时策略：DT 短超时（5s）保持教学交互性，HA 长超时（300s）支持批量 OCR。

---

## 硬件规格

| 组件 | 规格 | 角色 |
|------|------|------|
| SoC | Rockchip RK3576 (4×Cortex-A76 + 4×Cortex-A55) | CPU + NPU (6 TOPS) |
| RAM | 8GB LPDDR4X | 共享 CPU/GPU/NPU 内存 |
| 启动 | 16GB eMMC | OS + 内核 |
| 存储 | 256GB SSD | Docker 镜像、容器、用户数据 |

---

## 容器架构

```
┌────────────────────────────────────────────────────────────────────────┐
│                        Docker Network                                  │
│                   deepseek_deeptutor-network                            │
│                                                                         │
│  ┌──────────────────┐              ┌──────────────────────┐             │
│  │    微信(家长/孩子) │              │  DT Web UI (3782)    │             │
│  │    首要入口        │              │  深度学习补充入口     │             │
│  └────────┬─────────┘              └──────────────────────┘             │
│           │ iLink 长轮询                                               │
│           ▼                                                            │
│  ┌──────────────────────────────────────┐                              │
│  │         hermes_agent (8004)           │                              │
│  │  ┌─────────────────────────────────┐  │                              │
│  │  │  父网关 (parent bot)             │  │                              │
│  │  │  WEIXIN_TOKEN, 管理员权限        │  │                              │
│  │  └──────────────┬──────────────────┘  │                              │
│  │  ┌──────────────┴──────────────────┐  │                              │
│  │  │  子网关 (child bot)              │  │                              │
│  │  │  CHILD_WEIXIN_TOKEN, 学生权限    │  │                              │
│  │  └─────────────────────────────────┘  │                              │
│  │  ◆ 双网关 iLink 网关                │                              │
│  │  ◆ 家长/孩子双机器人身份             │                              │
│  │  ◆ 消息分类: OCR/教学/闲聊/设备      │                              │
│  │  ◆ teaching_sessions 会话管理        │                              │
│  │  ◆ 通知文件消费 (report_push)        │                              │
│  └──────────────────┬───────────────────┘                              │
│                     │ HTTP REST                                        │
│                     ▼                                                  │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │                   platform (8100/8101)                            │  │
│  │                                                                   │  │
│  │  ┌──────────── Provider API (8100) ────────────┐                 │  │
│  │  │  /api/tutor/chat     → DT WebSocket 教学    │                 │  │
│  │  │  /api/process/file   → OCR + 自动教学        │                 │  │
│  │  │  /api/ocr            → 纯 OCR               │                 │  │
│  │  │  /api/vision         → 视觉理解             │                 │  │
│  │  │  /api/solve          → 深度解题             │                 │  │
│  │  │  /api/vision/solve   → 拍照解题             │                 │  │
│  │  │  /api/ingest/*       → 知识库入库           │                 │  │
│  │  │  /api/kb/search      → ChromaDB 搜索        │                 │  │
│  │  │  /api/mastery/*      → 掌握度 CRUD          │                 │  │
│  │  │  /api/practice/*     → 练习/试卷生成        │                 │  │
│  │  │  /api/report/*       → 学习报告             │                 │  │
│  │  │  /api/llm/acquire    → LLM 锁获取           │                 │  │
│  │  │  /api/llm/release    → LLM 锁释放           │                 │  │
│  │  │  /api/bot/qrcode     → 微信二维码           │                 │  │
│  │  │  /api/bot/bind_child → 子网关绑定           │                 │  │
│  │  │  /health             → 健康检查             │                 │  │
│  │  └────────────────────────────────────────────┘                 │  │
│  │                                                                   │  │
│  │  ┌───────── MCP Server (同进程 ASGI) ──────────┐                 │  │
│  │  │  33+ 工具: kb_search, tutor_chat,           │                 │  │
│  │  │  process_file, deep_solve, wifi_configure,  │                 │  │
│  │  │  device_status, generate_exam_paper, ...    │                 │  │
│  │  │  ◆ 熔断器 (Circuit Breaker) 3次失败→切换    │                 │  │
│  │  │  ◆ 场景管理 (practice/reading/full)         │                 │  │
│  │  │  ◆ 角色权限 (家长/孩子工具门禁)             │                 │  │
│  │  └────────────────────────────────────────────┘                 │  │
│  │                                                                   │  │
│  │  ┌──────── Device Manager (8101) ─────────────┐                 │  │
│  │  │  /api/device/status   → CPU/内存/温度       │                 │  │
│  │  │  /api/device/storage  → 存储空间            │                 │  │
│  │  │  /api/device/ssd      → SSD 健康            │                 │  │
│  │  │  /api/device/wifi/*   → WiFi 扫描/连接      │                 │  │
│  │  │  /api/device/cleanup  → 清理临时文件         │                 │  │
│  │  └────────────────────────────────────────────┘                 │  │
│  │                                                                   │  │
│  │  tutor_platform/ (Python 模块):                                   │  │
│  │    unified_provider.py  — ChromaDB 单例封装                       │  │
│  │    ingest_status.py     — 入库状态追踪                            │  │
│  │    report_scheduler.py  — 报告调度                                │  │
│  │    report_push.py       — 报告格式化                              │  │
│  │    ha_client.py         — HA API 客户端 (cron 注册)              │  │
│  │    quiz_sync.py         — 答题记录同步                            │  │
│  │    storage.py           — 配置校验                                │  │
│  │    tools/embeddings.py  — Embedding 函数                          │  │
│  │    tools/intent_rules.py — 设备意图分类                           │  │
│  │    tools/preprocess.py  — 图片预处理                              │  │
│  └──────────────────┬────────────────────────────────────────────────┘  │
│                     │                                                  │
│            ┌────────┼────────────────┐                                 │
│            ▼        ▼                ▼                                 │
│  ┌────────────────┐  ┌──────────────────┐  ┌────────────────────────┐  │
│  │   rkllama      │  │    deeptutor      │  │  Domains (共享模块)    │  │
│  │   (8080)       │  │   (8001/3782)     │  │                        │  │
│  │  NPU LLM 服务   │  │  教学引擎         │  │  domains/tutoring/    │  │
│  │                │  │  ◆ AgentLoop      │  │   mastery.py          │  │
│  │  模型:         │  │  ◆ TutorBot WS    │  │   掌握度追踪          │  │
│  │  r1-distill    │  │  ◆ FastAPI 后端   │  │   错题本              │  │
│  │  deepseekocr   │  │  ◆ Next.js 前端   │  │   每日统计            │  │
│  │  qwen3-vl      │  │  ◆ 多用户系统     │  │   Ebbinghaus 复习     │  │
│  │  bge-small-zh  │  │  ◆ 知识库索引     │  │   家长报告            │  │
│  └────────────────┘  └──────────────────┘  └────────────────────────┘  │
│                                                                         │
│  图例:                                                                  │
│    ───→ HTTP / REST API                                                │
│    ───→ iLink 长轮询 (微信)                                            │
│    ───→ 本地 NPU LLM 调用                                               │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 端口映射

| 端口 | 容器 | 用途 |
|------|------|------|
| 8004 | hermes_agent | WeChat 双网关 API — **首要用户入口** |
| 3782 | deeptutor | 前端 Web UI — **深度学习补充入口** |
| 8001 | deeptutor | 后端 API (FastAPI + WebSocket) — 内部 |
| 8080 | rkllama | OpenAI 兼容 NPU LLM API — 内部 |
| 8100 | platform | Provider API + MCP Server — 内部 |
| 8101 | platform | Device Manager — 内部 |

端口 8004 对外开放给微信，其余均为 Docker 网络内部端口。

---

## 组件详情

### 1. hermes_agent (deepseek_hermes_agent) — 系统中枢 / 首要入口

微信 iLink 双网关。**中央协调器**：接收所有微信消息，分类后路由到对应后端。

**双网关架构：**
- **父网关** — 管理员权限 (WEIXIN_TOKEN)，可访问所有功能
- **子网关** — 学生权限 (CHILD_WEIXIN_TOKEN)，受限的教学交互

**核心机制 (HERMES PATCH 区域)：**

| 机制 | 角色 |
|------|------|
| `_teaching_sessions` dict | 跟踪活跃教学会话 (30min TTL) |
| `_auto_process_media` | 拦截图片 → platform OCR + 教学 |
| `_auto_teaching_followup` | 学生文字回复路由到 DT (非本地 LLM) |
| `_consume_report_notifications` | 后台任务 (30s 轮询) 消费通知文件 |
| `_process_message` | 主分发: 媒体→OCR, 文字→教学/闲聊 |

**教学会话生命周期：**
1. 学生发图片 → `_auto_process_media` → POST /api/process/file → OCR + DT 教学 → 会话创建
2. 学生回复文字 → `_auto_teaching_followup` → POST /api/tutor/chat → DT 评估 + 下一题
3. 解析 DT 回复中的答案评估标记 → 更新掌握度 → 自动触发练习或试卷
4. 30分钟无活动 → 会话过期 → 恢复正常聊天

---

### 2. platform (deepseek_platform) — 编排层 / MCP / 设备管理

编排层和 **LLM 调度器** (原则 1)。接收 HA 请求，协调 OCR、LLM 调度、教学。

#### LLM 资源锁

`_llm_lock` (`asyncio.Lock`) 序列化本地 NPU 访问：

| 场景 | 超时 | 结果 |
|------|------|------|
| DT 教学，锁空闲 | 5s | 获取 → 切 DT 到 rkllama 配置 → 本地 NPU 教学 |
| DT 教学，锁被占 | 5s 超时 | 切 DT 到 deepseek 配置 → 云端 API 教学 |
| HA OCR，锁空闲 | 300s | 获取 → OCR → 释放 |
| HA OCR，锁被占 | 300s | 等待 (DT 教学通常 30-60s 完成) |

#### MCP Server (同进程 ASGI 合并)

Phase B 合并：MCP Server 作为 ASGI middleware 内嵌在 platform 进程中，通过 `httpx.ASGITransport` 直接调用本进程 API，无网络开销。

**33+ MCP 工具分类：**

| 类别 | 工具 | 数量 |
|------|------|------|
| 知识库 | kb_search, kb_list, kb_upload_text, kb_upload_file | 4 |
| 教学 | tutor_chat, deep_solve, vision_solve, process_file | 4 |
| 练习 | generate_practice, generate_exam_paper, quiz_review | 3 |
| 教材 | book_create, book_read, book_list | 3 |
| 笔记 | notebook_create, notebook_read, notebook_add_cell, notebook_execute, notebook_list | 5 |
| 学习记录 | record_quiz_result, question_notebook_list, session_read, get_memory, update_memory | 5 |
| 报告 | generate_report, push_report | 2 |
| 写作 | cowriter_edit, cowriter_documents, cowriter_history | 3 |
| 设备管理 | device_status, storage_info, ssd_health, device_temp, device_alerts, device_cleanup, device_command, wifi_scan, wifi_configure, wifi_status, wifi_forget, get_bot_qrcode, bind_child_bot | 13 |
| 场景 | set_scene, detect_scene, get_scene | 3 |
| 熔断器 | get_circuit_status, reset_circuit | 2 |

**关键特性：**

- **熔断器 (Circuit Breaker)** — 每个下游服务 (deeptutor/platform/device_manager) 独立熔断器，3 次连续失败 → 开启 → 60s 恢复
- **场景管理** — practice/reading/full 三种模式，限制可用工具集
- **角色权限门禁** — 孩子无法调用设备管理工具 (`device_command`, `wifi_configure` 等)
- **慢操作队列** — `asyncio.Semaphore(1)` 串行化 CPU/IO 密集型操作

#### Device Manager (8101)

平台容器内的子服务，通过 subprocess 管理 RK3576 硬件：

| 端点 | 功能 |
|------|------|
| `/api/device/status` | CPU/内存/温度综合状态 |
| `/api/device/storage` | 存储空间详情 (挂载点使用率) |
| `/api/device/ssd` | SSD 健康度、写入量、磨损均衡 |
| `/api/device/temp` | SoC 温度 (thermal zones) |
| `/api/device/alerts` | 当前活跃告警 |
| `/api/device/wifi/scan` | 扫描 WiFi 网络 |
| `/api/device/wifi/connect` | 连接 WiFi |
| `/api/device/wifi/status` | 当前连接状态 |
| `/api/device/wifi/forget` | 忘记网络 |
| `/api/device/cleanup` | 清理临时文件和旧日志 |

#### 掌握度数据模型 (`domains/tutoring/mastery.py`)

JSON 文件存储 (`/data/mastery/{learner_id}.json`)：

| 字段 | 类型 | 用途 |
|------|------|------|
| `mastery` | `Dict[kp_id, {level, total, correct}]` | 每知识点掌握度 (0.0–1.0) |
| `wrong_answers` | `List[Dict]` | 最近 50 条错题 |
| `daily_stats` | `Dict[YYYY-MM-DD, {total, correct, wrong, weak_points}]` | 每日聚合统计 |
| `answer_history` | `List[Dict]` | 最近 500 条答题记录 |
| `review_schedule` | `Dict[kp_id, due_date]` | Ebbinghaus 复习计划 |
| `review_history` | `List[Dict]` | 最近 200 条复习记录 |

自动迁移：加载时检测并执行 `_migrate_v1()`。

#### 答案评估标记

DT 在回复末尾添加结构化标记：
```
[ANSWER:correct:kp_id]  或   [ANSWER:wrong:kp_id]
```

platform 的 `_parse_answer_evaluation()` 解析流程：
1. 剥离标记，返回清洗后的内容
2. 提取 `(result, kp_id)` 元组
3. 调用 `update_mastery()` 更新掌握度、每日统计、答题历史
4. 调用 `schedule_review()` 安排 Ebbinghaus 复习

#### 自动练习触发

当同一知识点连续答错 ≥2 次：
1. 直接调用 DeepSeek API (绕过 HA 避免模型名剥离问题)
2. 生成 3 道针对性练习题
3. 通过 `pending_practice` 字段返回，HA 发送到微信

#### 自动试卷生成

当薄弱知识点 ≥3 个 (水平 < 0.6)：
1. 后台任务 `_auto_generate_exam()` 触发 (24h 冷却)
2. 读取全部薄弱点和错题记录
3. 调用 DeepSeek API 生成 3 段式试卷 (选择题/填空题/解答题)
4. 存入 `_pending_exam_context[learner_id]`
5. 下次教学时注入 SOUL.md

#### Ebbinghaus 间隔复习

| 掌握度 | 间隔 | 分类 |
|--------|------|------|
| < 0.4 | 1 天 | 薄弱 |
| 0.4–0.6 | 3 天 | 学习中 |
| 0.6–0.8 | 7 天 | 进步中 |
| 0.8–0.9 | 14 天 | 已掌握 |
| ≥ 0.9 | 30 天 | 已巩固 |

每次教学前，`_update_soul_with_context()` 查询到期复习并注入 SOUL.md。

#### SOUL.md 系统

两种教学人格，通过 HTTP PATCH `/api/v1/tutorbot/teacher` 更新 DT 的 SOUL.md：

| 常量 | 模式 | 用途 |
|------|------|------|
| `_TEACHER_SOUL` | guide | 苏格拉底式引导教学 |
| `_TEACHER_EXPLAIN_SOUL` | explain | 直接讲解模式 |

**SOUL.md 注入内容 (每次教学前)：**
- 当前教学上下文 (OCR 提取的题目)
- 相关知识库参考 (ChromaDB 查询，top 3，去重)
- 到期复习知识点 (Ebbinghaus)
- 薄弱知识点列表
- 近期错题记录
- 待注入的自动生成试卷

---

### 3. deeptutor (deepseek_deeptutor) — 教学工具 / 深度学习 UI

HA 通过 platform 调用的专业教学引擎。Web UI (3782) 提供深度学习补充入口。

**核心能力：**
- `TutorBot` — 苏格拉底式教学 Agent，每次交互读取 SOUL.md
- WebSocket `/api/v1/tutorbot/teacher/ws` — 实时教学 (仅 platform 调用)
- AgentLoop 按 catalog 配置直接调用 LLM

**模型 Catalog：**

| 配置 | 模型 | 端点 |
|------|------|------|
| `deepseek` | deepseek-v4-flash | `https://api.deepseek.com/v1` |
| `rkllama` | r1-distill-1.5b | `http://rkllama:8080/v1` |

Platform 在每次 WS 教学前切换配置。DT 无锁意识——只按 catalog 调用。

---

### 4. rkllama (deepseek_rkllama)

NPU LLM 服务，运行在 RK3576 NPU 上。

**可用模型：**

| 模型 | 参数量 | 类型 | RAM | 加载 |
|------|--------|------|-----|------|
| r1-distill-1.5b | 1.5B | text | 591 MB | 常驻 |
| deepseekocr-3b | 3B | OCR | 1.8 GB | 惰性 |
| qwen3-vl-2b | 2B | vision | 1.1 GB | 惰性 |
| bge-small-zh | — | embedding | — | — |

非文本模型惰性加载以节省内存。

---

## 数据流

### 教学流程

```
微信图片 ──→ hermes_agent (系统中枢)
                  │
                  ├─ _auto_process_media()
                  │    │
                  │    ├─ POST /api/llm/acquire?timeout=300s
                  │    │    ├─ 成功 → 持有锁做 OCR
                  │    │    └─ 失败 → OCR 跳过 (罕见)
                  │    │
                  │    ├─ POST /api/process/file
                  │    │    │
                  │    │    ├─ OpenCV 预处理 → rkllama OCR
                  │    │    │    └─ OCR 结果:
                  │    │    │        ├─ 有内容 → 入库知识库
                  │    │    │        └─ 空/失败 → 引导用户重新输入
                  │    │    │
                  │    │    └─ auto_teach=true 且内容为教育 → _tutor_chat_core()
                  │    │         │
                  │    │         ├─ _llm_lock.acquire(timeout=5s)
                  │    │         │    ├─ 成功 → 切 DT 到 rkllama
                  │    │         │    └─ 超时 → 切 DT 到 deepseek
                  │    │         │
                  │    │         └─ DT WebSocket 教学
                  │    │              └─ DT AgentLoop → LLM (本地/云端)
                  │    │
                  │    └─ 教学会话创建
                  │
                  └─ 通知文件写入 → HA 消费后推送微信

微信文字 ──→ hermes_agent
                  │
                  ├─ _is_teaching_session() = true
                  │
                  └─ _auto_teaching_followup()
                       │
                       └─ POST /api/tutor/chat
                            │
                            └─ _tutor_chat_core()
```

### 学习增强数据流

```
答案评估 (每次教学交互):
  DT WebSocket 回复
       │
       ├─ _parse_answer_evaluation()
       │    └─ 剥离 [ANSWER:correct|wrong:kp_id] 标记
       │
       ├─ update_mastery(kp_id, correct)
       │    ├─ 更新 KPI 掌握度
       │    ├─ 记录答题历史
       │    ├─ 更新每日统计
       │    ├─ 记录错题 (如适用)
       │    └─ schedule_review() → Ebbinghaus 间隔
       │
       ├─ _trigger_practice_if_needed()
       │    └─ 同 KPI 连续答错 ≥2 → 生成 3 道巩固题
       │
       └─ _auto_generate_exam() (后台)
            └─ 薄弱点 ≥3 & 24h 冷却 → 生成强化试卷
                 └─ 存入 _pending_exam_context

SOUL.md 注入 (每次教学前):
  _update_soul_with_context()
       │
       ├─ ChromaDB 查询相关知识 → 注入
       ├─ get_due_reviews() → Ebbinghaus 到期复习
       ├─ weak_points() → 薄弱知识点摘要
       ├─ get_wrong_answers() → 近期错题
       └─ _pending_exam_context → 自动生成的试卷

报告推送:
  HA cron 触发 ──→ ha_client.py 注册
       │
       ├─ report_scheduler.py
       │    ├─ enumerate_learners()
       │    ├─ generate_parent_report(days=N)
       │    └─ _write_notification() → /data/hermes/notifications/
       │
       └─ weixin.py _consume_report_notifications()
            └─ self.send() → 微信用户
```

---

## 文件处理流水线

```
上传文件
    │
    ├─ 分类 (按扩展名)
    │    │
    │    ├─ 图片 (.jpg/.png/…)
    │    │    └─ OpenCV 预处理 (去偏斜 → 去噪 → CLAHE 增强 → 自适应二值化)
    │    │         └─ rkllama OCR → 文本
    │    │
    │    ├─ PDF
    │    │    ├─ 含文本层 → markitdown 提取
    │    │    └─ 扫描件 → 每页渲染 PNG → OpenCV → OCR
    │    │
    │    ├─ Office (.docx/.pptx/.xlsx)
    │    │    └─ markitdown 提取
    │    │
    │    ├─ 旧版 .doc
    │    │    └─ antiword CLI 提取
    │    │
    │    └─ 文本 (.txt/.md/.html)
    │         └─ 直接读取 (HTML 做 XSS 消毒)
    │
    ├─ 教育内容检测
    │    └─ 关键词 + 格式特征启发式判断
    │
    ├─ 异步双写入向量存储
    │    ├─ 平台 ChromaDB (PersistentClient, 内嵌)
    │    │    └─ 按段落分块 (≤500 字符), 带 metadata 入库
    │    └─ DT LlamaIndex (HTTP POST /api/v1/knowledge)
    │         └─ 写入临时文件 → 上传到知识库
    │
    ├─ 缓存教学上下文到内存 + 持久化到磁盘
    │
    ├─ 写入通知文件 → HA 消费通知微信
    │
    └─ auto_teach=true → 自动触发 _tutor_chat_core()
```

---

## 微信实时性保障

| 措施 | 详情 |
|------|------|
| 教学超时 | DT WebSocket 响应预期 30s 内 |
| 锁超时 (DT) | 5s → 本地 NPU 忙则立即切换云端 |
| 锁超时 (HA) | 300s — OCR 可等待，用户看到"处理中" |
| 模型惰性加载 | OCR (1.8GB) 和视觉 (1.1GB) 按需加载 |
| OCR 预热 | 首次图片请求预热一次，持久化标记跳过后续预热 |
| OCR 并发控制 | `asyncio.Semaphore(2)` 防 NPU OOM |
| 会话保活 | HA 维护 iLink 心跳 |
| WS 连接复用 | `_DTTutorSession` 连接池，避免每次 2-5s 冷启动 |
| WS 空闲清理 | 每 5 分钟清理空闲 >30 分钟的会话 |
| 会话自动清理 | 每日凌晨 4-6 点发送 /new 防 OOM |
| 上下文持久化 | 磁盘 JSON 文件 + 启动恢复 |
| 本地 LLM 弱回复兜底 | 本地模型输出 < 20 字符时自动降级到云端 |

---

## 存储布局

```
宿主机路径                    容器路径                        用途
──────────                    ─────────────                  ───────
./data/user                  /app/data/user                  Model catalog, 用户设置
./data/memory                /app/data/memory                DT 记忆持久化
./data/knowledge_bases       /app/data/knowledge_bases       RAG 知识库
./data/mastery               /data/mastery                   学习者掌握度数据
./data/chromadb              /data/chromadb                  向量存储
./data/ingest_status         /data/ingest_status             文件入库追踪
./data/uploads               /data/uploads                   用户上传
./data/sources               /data/sources                   处理后归档
./data/hermes/notifications  /data/hermes/notifications      报告推送通知文件
./data_dev/hermes            /opt/data                       父网关数据
./data_dev/hermes_child      /opt/data/child                 子网关数据

代码挂载 (只读):
./docker/platform/provider_api.py   → /app/provider_api.py
./docker/platform/mcp_server.py     → /app/mcp_server.py
./vendor/hermes-agent/run_agent.py  → /opt/hermes/run_agent.py
./vendor/hermes-agent/.../weixin.py → /opt/hermes/gateway/platforms/weixin.py
./docker/platform/patches/gateway_run.py → /opt/hermes/gateway/run.py
```

---

## 关键设计决策

1. **微信优先，Web 补充** — 日常学习通过微信完成，Web UI 仅用于深度学习

2. **HA 为系统中枢** — HA 拥有会话生命周期和路由逻辑，DT 是纯教学工具

3. **本地优先，透明降级** — 5s 锁超时后自动切换到云端，用户无感知

4. **差异化锁超时** — DT 5s 短超时保持交互性，HA 300s 长超时支持 OCR

5. **DT 直连 LLM，platform 控制路由** — DT AgentLoop 直接调用 LLM，platform 负责切换 catalog 配置

6. **SOUL.md 承载教学策略** — 所有教学规则在 SOUL.md，DT 每轮重新读取

7. **最小 HA 补丁** — 所有 HA 修改用 `==== HERMES PATCH` 标记包裹

8. **NPU 模型惰性加载** — 仅 text 模型常驻，OCR/vision 按需加载

9. **[ANSWER:correct|wrong] 评估标记** — DT 仅评估，platform 持久化

10. **DeepSeek API 直连** — 练习/试卷生成绕过 HA 代理，避免 502 错误

11. **通知文件桥** — `report_scheduler.py` 写文件 → HA 消费发送，解耦两个容器

12. **自动试卷 24h 冷却** — 防止过度推送，确保学习节奏自然

13. **Ebbinghaus 复习融入教学流** — 复习安排注入 SOUL.md，学习中自然复习

14. **MCP 合并到 platform 进程** — Phase B 同进程 ASGI，减少网络开销

15. **双网关 WeChat** — 父 + 子独立 iLink 会话，角色权限分离

16. **WS 连接池** — 按 learner 复用 WebSocket，避免每轮教学 2-5s 冷启动

17. **上下文磁盘持久化** — 容器重启后恢复教学上下文，从中断处继续

18. **教育与 OCR 并发控制** — `Semaphore(2)` 防止 NPU 内存溢出

19. **基于规则 + LLM 兜底的设备管理** — `device_command` 先规则匹配，未分类时 LLM 处理

20. **文件归档 trace_id 体系** — 所有上传文件以 `{trace_id}_{timestamp}_{filename}` 归档，支持 `view_source` 溯源
