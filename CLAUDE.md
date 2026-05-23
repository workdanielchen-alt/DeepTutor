# DeepTutor Project — Development Rules

## 1. 文件修改权限

### 🔴 禁止修改

| 目录 | 说明 |
|------|------|
| `deeptutor/` | 教学引擎核心，后续开发保持不改 |
| `tests/` | 测试文件，除非你明确要求 |

### 🟡 尽量少改 (改必有 patch)

| 目录 | 说明 |
|------|------|
| `vendor/hermes-agent/` | 微信 iLink 双网关 |

如需修改 hermes-agent：
1. 优先用外部配置 (`config/`) 实现
2. 必须改源码时，用 patch 文件追踪：
   - `git diff vendor/hermes-agent/xxx.py > patches/xxx.patch`
   - 升级后用 `git apply patches/xxx.patch` 快速恢复
3. 不改函数签名，不封装接口层

### 🟢 可正常修改 (项目自有代码)

`docker/platform/` `tutor_platform/` `domains/` `web/` `scripts/` `docker-compose*.yml`
`ARCHITECTURE.md` `PRD.md` `CLAUDE.md`

## 2. 分支策略

- 新功能/改动在 **feature branch** 上开发
- `master` 分支只合入你确认的变更
- 不直接向 master 推送未确认的改动

## 3. 协作约定

1. **先计划，后动手** — 所有改动我先出计划，你点头再执行
2. **改完预览** — 执行后先给你看 diff，你再决定是否提交
3. **只做你明确要求的任务** — 不擅自重构、优化、加测试、改配置
4. **小步提交** — 一次一个目标，不混入无关变更
5. **不留垃圾** — 工作区保持干净，临时文件及时清理
