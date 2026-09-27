# 真实工程案例脱敏交付

纯 Python 服务端项目：角色分明的交付工作流 + 只追加事件溯源 + SQLite 持久化 + JSON API 边界。

## 角色与流程

- **提交人（submitter）**：登记案例、提交进入复核、退回后重新提交
- **脱敏复核人（reviewer）**：退回（必须留原因）或复核通过
- **发布人（publisher）**：发布复核通过的案例

状态机：`draft → in_review →（returned ⇄ in_review）→ approved → published`，`published` 为终态。
每个命令都校验调用者角色，越权返回 403，状态不允许返回 409。

## 核心保证

- **每步留痕**：登记/提交/退回/通过/发布各追加一条事件（含操作人、角色、原因、时间、版本号），当前状态只是事件的投影。
- **决定不可覆盖**：仓储只有追加与查询；数据库触发器对 UPDATE/DELETE 直接 `RAISE(ABORT)`。
- **并发不冲突**：`UNIQUE(case_id, version)` 乐观并发 + `one_publish_per_case` 部分唯一索引，同一案例全库最多一条发布事件；命令可携带 `expected_version` 做乐观锁。
- **可还原**：`GET /cases/{id}/history` 按版本顺序返回全部决定记录。
- **重启不丢**：事件落 SQLite，服务启动时回放全部事件重建状态。

## API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/cases` | 登记案例 `{id, actor, role, idempotency_key?}` |
| POST | `/cases/{id}/submit` | 提交（可带 `expected_version`、`idempotency_key`） |
| POST | `/cases/{id}/return` | 退回（可带 `reason`） |
| POST | `/cases/{id}/approve` | 复核通过 |
| POST | `/cases/{id}/publish` | 发布 |
| GET | `/cases`、`/cases/{id}` | 当前状态 |
| GET | `/cases/{id}/history` | 完整决定历史（审核入口） |

错误码：400 参数缺失 / 403 角色不符 / 404 案例不存在 / 409 状态冲突或版本冲突。

测试命令：python3 -m unittest discover -s tests -v

编译命令：python3 -m compileall -q service_09261_003 tests
