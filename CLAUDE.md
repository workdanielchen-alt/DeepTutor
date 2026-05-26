# DeepTutor Project — Development Rules

## 1. 文件修改权限

### 🔴 禁止修改

| 目录 | 说明 |
|------|------|
| 目录 | 说明 |
|------|------|
| `vendor/deeptutor/deeptutor/` | 教学引擎核心 (HKUDS/DeepTutor)，不直接修改 |
| `vendor/deeptutor/deeptutor_cli/` | CLI 层 (HKUDS/DeepTutor)，不直接修改 |
| `vendor/deeptutor/deeptutor_web/` | Web 包 (HKUDS/DeepTutor)，不直接修改 |
| `tests/` | 不擅自修改已有测试；新功能需同步增加对应测试 |

### 🟡 尽量少改 (改必有 patch)

| 目录 | 说明 |
|------|------|
| `vendor/hermes-agent/` | 微信 iLink 双网关 |

如需修改 vendor 代码：
1. 优先用外部配置 (`config/`) 实现
2. 必须改源码时，用 patch 文件追踪：
   - `git diff vendor/xxx.py > patches/xxx.patch`
   - 升级后用 `git apply patches/xxx.patch` 快速恢复
3. 不改函数签名，不封装接口层

当前已有 patch 文件：
- `patches/deeptutor-math-animator-config.patch` — math_animator 配置兼容
- `patches/deeptutor-rkllama-embedding.patch` — rkllama embedding provider
- `patches/hermes-wechat-vendor.patch` — 通知路由 + 文件确认 + HA 其他修改 (原 hermes-wechat-file-ack + hermes-wechat-notification-routing 合并)
- `patches/knowledge-list-async.patch` — `/api/v1/knowledge/list` 同步 I/O 阻塞修复：将 KB 信息加载移至线程池

### 🟢 可正常修改 (项目自有代码)

`docker/platform/` `tutor_platform/` `domains/` `web/` `scripts/` `patches/` `docker-compose*.yml`
`ARCHITECTURE.md` `PRD.md` `CLAUDE.md`

## 4. 上游代码升级流程 (vendor/deeptutor)

```bash
# 1. 拉取最新上游代码
git fetch upstream

# 2. 将上游变更复制到 vendor（upstream 仍是原始路径）
git checkout upstream/main -- deeptutor/ deeptutor_cli/ deeptutor_web/
cp -a deeptutor/* vendor/deeptutor/deeptutor/
cp -a deeptutor_cli/* vendor/deeptutor/deeptutor_cli/
cp -a deeptutor_web/* vendor/deeptutor/deeptutor_web/

# 3. 恢复本地 patch
git apply patches/*.patch

# 4. 重新注册可编辑安装
pip install -e .

# 5. 清理临时文件
git checkout -- deeptutor/ deeptutor_cli/ deeptutor_web/  # 还原根目录副本

# 6. 验证
python -c "from deeptutor.app import DeepTutorApp; print('OK')"
pytest tests/ -x -q
```

## 2. 分支策略

- 新功能/改动在 **feature branch** 上开发
- `master` 分支只合入你确认的变更
- 不直接向 master 推送未确认的改动

## 3. 协作约定

1. **先计划，后动手** — 所有改动我先出计划，你点头再执行
2. **改完预览** — 执行后先给你看 diff，你再决定是否提交
3. **修改必须测试验证** — 提交前确保改动不影响现有功能，有测试的模块要跑通测试
4. **新功能同步加测试** — 新增功能时需编写对应测试用例
5. **只做你明确要求的任务** — 不擅自重构、优化、改配置
6. **小步提交** — 一次一个目标，不混入无关变更
7. **不留垃圾** — 工作区保持干净，临时文件及时清理
