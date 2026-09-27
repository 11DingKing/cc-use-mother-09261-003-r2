"""JSON API 适配器：角色分明的命令端点 + 审计查询。

命令端点（均为 POST，body 中带 actor、role）：
    POST /cases                 提交人建档
    POST /cases/{id}/submit     提交人提交 / 退回后重新提交
    POST /cases/{id}/return     复核人退回（body.reason 必填）
    POST /cases/{id}/approve    复核人复核通过
    POST /cases/{id}/publish    发布人发布

查询端点：
    GET  /cases                 案例列表（当前状态）
    GET  /cases/{id}            单个案例当前状态
    GET  /cases/{id}/history    完整决策历史（审计用）

状态码：201 新建决定 / 200 幂等重放或查询 / 403 角色越权 /
404 不存在 / 409 并发冲突 / 422 状态或参数不合法。
"""
from __future__ import annotations

import json

from .store import ConcurrencyConflict
from .workflow import (
    NotFoundError,
    RoleError,
    StateError,
    Workflow,
    WorkflowError,
)


def dispatch(flow: Workflow, method: str, path: str, body: dict | None = None):
    body = body or {}
    parts = [p for p in path.strip("/").split("/") if p]

    try:
        # ---------- 命令 ----------
        if method == "POST" and parts == ["cases"]:
            event, replayed = flow.create_case(
                body["id"], body["actor"],
                body.get("role", "submitter"),
                content=body.get("content"),
                idempotency_key=body.get("idempotency_key"),
            )
            return _decision_response(event, replayed)

        if method == "POST" and len(parts) == 3 and parts[0] == "cases":
            case_id, action = parts[1], parts[2]
            common = dict(
                actor=body["actor"],
                role=body.get("role"),
                expected_version=body.get("expected_version"),
                idempotency_key=body.get("idempotency_key"),
            )
            if action == "submit":
                event, replayed = flow.submit(
                    case_id, content=body.get("content"),
                    comment=body.get("comment"),
                    **{k: v for k, v in common.items() if v is not None or k in ("actor",)},
                )
            elif action == "return":
                event, replayed = flow.return_case(
                    case_id, reason=body.get("reason", ""),
                    **{k: v for k, v in common.items() if v is not None or k == "actor"},
                )
            elif action == "approve":
                event, replayed = flow.approve(
                    case_id, comment=body.get("comment"),
                    **{k: v for k, v in common.items() if v is not None or k == "actor"},
                )
            elif action == "publish":
                event, replayed = flow.publish(
                    case_id, comment=body.get("comment"),
                    **{k: v for k, v in common.items() if v is not None or k == "actor"},
                )
            else:
                return 404, {"error": "not_found"}
            return _decision_response(event, replayed)

        # ---------- 查询 ----------
        if method == "GET" and parts == ["cases"]:
            return 200, {"cases": flow.list_cases()}

        if method == "GET" and len(parts) == 2 and parts[0] == "cases":
            return 200, flow.get_case(parts[1]).to_dict()

        if method == "GET" and len(parts) == 3 and parts[0] == "cases" and parts[2] == "history":
            return 200, flow.history(parts[1])

        return 404, {"error": "not_found"}

    except ConcurrencyConflict as exc:
        return 409, {"error": "concurrency_conflict", "message": str(exc)}
    except RoleError as exc:
        return 403, {"error": "forbidden_role", "message": str(exc)}
    except NotFoundError as exc:
        return 404, {"error": "not_found", "message": str(exc)}
    except (StateError, WorkflowError) as exc:
        return 422, {"error": "unprocessable", "message": str(exc)}
    except KeyError as exc:
        return 400, {"error": "bad_request", "message": f"缺少必填字段: {exc.args[0]}"}


def _decision_response(event: dict, replayed: bool):
    return (200 if replayed else 201), {
        "idempotent_replay": replayed,
        "decision": event,
    }


def handle_json(flow: Workflow, method: str, path: str, raw_body: bytes | None = None):
    """供 HTTP 层调用：解析 JSON 字节，返回 (status, headers, body_bytes)。"""
    body = {}
    if raw_body:
        try:
            body = json.loads(raw_body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return 400, {"error": "bad_request", "message": "请求体不是合法 JSON"}
    status, payload = dispatch(flow, method, path, body)
    return status, payload
