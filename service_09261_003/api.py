"""JSON API 适配器：把请求映射为工作流命令，把领域错误映射为状态码。

路由一览：
    POST /cases                登记案例（提交人）
    POST /cases/{id}/submit    提交进入复核（提交人）
    POST /cases/{id}/return    退回（脱敏复核人，body 可带 reason）
    POST /cases/{id}/approve   复核通过（脱敏复核人）
    POST /cases/{id}/publish   发布（发布人）
    GET  /cases                全部案例当前状态
    GET  /cases/{id}           单个案例当前状态
    GET  /cases/{id}/history   完整决定历史（审核还原入口）
"""
from .workflow import WorkflowError

_ACTIONS = ("submit", "return", "approve", "publish")


def dispatch(flow, method, path, body=None):
    """无框架的 JSON 边界：返回 (status, payload)。"""
    body = body or {}
    parts = [p for p in path.split("/") if p]
    try:
        if method == "POST" and parts == ["cases"]:
            return 201, flow.register(body["id"], body["actor"], body["role"],
                                      idempotency_key=body.get("idempotency_key"))
        if method == "GET" and parts == ["cases"]:
            return 200, flow.snapshot()
        if len(parts) >= 2 and parts[0] == "cases":
            case_id = parts[1]
            if method == "GET" and len(parts) == 2:
                return 200, flow.case(case_id)
            if method == "GET" and len(parts) == 3 and parts[2] == "history":
                return 200, flow.history(case_id)
            if method == "POST" and len(parts) == 3 and parts[2] in _ACTIONS:
                return 200, _run(flow, parts[2], case_id, body)
        return 404, {"error": "not_found"}
    except WorkflowError as exc:
        return exc.status, {"error": exc.code, "message": str(exc)}
    except (KeyError, TypeError) as exc:
        return 400, {"error": "bad_request", "message": str(exc)}


def _run(flow, action, case_id, body):
    common = dict(expected_version=body.get("expected_version"),
                  idempotency_key=body.get("idempotency_key"))
    if action == "submit":
        return flow.submit(case_id, body["actor"], body["role"], **common)
    if action == "return":
        return flow.return_case(case_id, body["actor"], body["role"],
                                reason=body.get("reason", ""), **common)
    if action == "approve":
        return flow.approve(case_id, body["actor"], body["role"], **common)
    return flow.publish(case_id, body["actor"], body["role"], **common)
