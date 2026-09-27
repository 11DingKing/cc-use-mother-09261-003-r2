# 真实工程案例脱敏交付

把脱敏案例交付给外部教师的**角色分明**工作流服务。每次提交、退回、复核、发布
都追加一条不可变决定记录；谁在哪一步作的决定，事后可通过审计接口完整还原。

## 角色与状态流转

| 角色 | 可执行动作 |
| --- | --- |
| `submitter` 提交人 | 建档、提交送审、退回后修改再提交 |
| `reviewer` 复核人 | 复核通过、退回（**必须填写原因**） |
| `publisher` 发布人 | 将复核通过的案例发布给外部教师 |

```
draft ──submit──▶ submitted ──approve──▶ approved ──publish──▶ published
                      │
                   return
                      ▼
                   returned ──submit──▶ submitted（重新送审）
```

## 关键保证

- **决定不可覆盖（事件溯源 + 仅追加）**：所有决定写入只追加的 `events` 表，
  表上的触发器在数据库层拒绝任何 `UPDATE` / `DELETE`。退回后再提交是**新记录**，
  旧版本内容、意见、指纹全部保留。
- **角色隔离**：每个动作只允许指定角色执行，越权返回 `403`。
- **并发安全**：写事务使用 `BEGIN IMMEDIATE`，配合 `(case_id, version)` 乐观锁，
  基于过期版本的决定返回 `409`；Published 事件上的部分唯一索引保证
  **同一案例并发发布时数据库层只允许一个成功**，不会产生两个互相冲突的发布结果。
- **重启不丢历史**：SQLite 文件持久化（默认 `cases.db`），当前状态完全由重放
  事件得到；重启后历史与版本号原样保留。
- **幂等支持**：请求可带 `idempotency_key`，超时重试返回首次决定且不产生重复记录。

## HTTP 接口

启动：

```bash
CASE_DB_PATH=cases.db PORT=8080 python3 -m service_09261_003.server
```

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/cases` | 提交人建档 |
| POST | `/cases/{id}/submit` | 提交 / 退回后重新提交 |
| POST | `/cases/{id}/return` | 复核人退回（body.`reason` 必填） |
| POST | `/cases/{id}/approve` | 复核人通过 |
| POST | `/cases/{id}/publish` | 发布人发布 |
| GET | `/cases` | 案例当前状态列表 |
| GET | `/cases/{id}` | 单个案例当前状态 |
| GET | `/cases/{id}/history` | **完整决策历史（审计接口）** |

命令请求体示例（角色字段 `role` 默认值与该端点要求的角色一致）：

```json
POST /cases/C-100/submit
{"actor": "张提交", "role": "submitter", "content": {"project": "某市桥梁"},
 "comment": "坐标已模糊化", "expected_version": 3, "idempotency_key": "req-001"}
```

历史记录每条包含：版本号、动作、执行后状态、操作人、角色、载荷
（内容、退回原因、意见、内容 SHA-256 指纹）、决定时间（UTC）。

状态码：`201` 新决定 / `200` 幂等重放或查询 / `400` 参数缺失 /
`403` 角色越权 / `404` 案例不存在 / `409` 并发冲突 / `422` 状态或参数不合法。

## 代码结构

- `store.py`：SQLite 仅追加事件仓储（触发器防改写、唯一索引防重复发布、写事务串行化）
- `workflow.py`：角色状态机，无内存可变状态，一切由重放事件得到
- `api.py`：JSON 分发适配器（状态码与错误模型）
- `server.py`：标准库零依赖 HTTP 入口

测试命令：`python3 -m unittest discover -s tests -v`

编译命令：`python3 -m compileall -q service_09261_003 tests`
