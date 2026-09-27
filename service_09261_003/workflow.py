"""角色分明的真实工程案例脱敏交付工作流。

采用事件溯源：登记、提交、退回、通过、发布，每个决定都追加一条新事件，
已写入的事件永不修改或删除；当前状态只是全部事件的投影，
重启后由事件回放重建，因此历史天然完整、可审计。
"""
import sqlite3
import threading
from datetime import datetime, timezone


class Role:
    """交付流程中的三类角色，各司其职。"""
    SUBMITTER = "submitter"  # 提交人
    REVIEWER = "reviewer"    # 脱敏复核人
    PUBLISHER = "publisher"  # 发布人


class State:
    """案例生命周期状态（由最后一条事件推导，不单独存储）。"""
    DRAFT = "draft"          # 已登记，待提交
    IN_REVIEW = "in_review"  # 已提交，待脱敏复核
    RETURNED = "returned"    # 已退回，待重新提交
    APPROVED = "approved"    # 复核通过，待发布
    PUBLISHED = "published"  # 已发布（终态）


# 事件类型 -> (所需角色, 允许的前置状态, 到达状态)；前置状态为 None 表示仅允许全新案例。
ACTIONS = {
    "registered": (Role.SUBMITTER, None, State.DRAFT),
    "submitted": (Role.SUBMITTER, frozenset({State.DRAFT, State.RETURNED}), State.IN_REVIEW),
    "returned": (Role.REVIEWER, frozenset({State.IN_REVIEW}), State.RETURNED),
    "approved": (Role.REVIEWER, frozenset({State.IN_REVIEW}), State.APPROVED),
    "published": (Role.PUBLISHER, frozenset({State.APPROVED}), State.PUBLISHED),
}


class WorkflowError(Exception):
    """工作流错误基类，status/code 供 API 层映射为响应。"""
    status = 400
    code = "workflow_error"


class UnknownCase(WorkflowError):
    status = 404
    code = "unknown_case"


class DuplicateCase(WorkflowError):
    status = 409
    code = "duplicate_case"


class RoleNotAllowed(WorkflowError):
    status = 403
    code = "role_not_allowed"


class InvalidTransition(WorkflowError):
    status = 409
    code = "invalid_transition"


class VersionConflict(WorkflowError):
    status = 409
    code = "version_conflict"


class Workflow:
    """案例交付工作流：校验角色与状态机后，把每个决定追加为不可变事件。"""

    def __init__(self, store):
        self._store = store
        self._lock = threading.RLock()
        self._cases = {}  # case_id -> {"state": ..., "version": ...}（事件投影）
        for event in store.all_events():  # 重启后回放全部历史，重建投影
            self._apply(event)

    # ---- 命令：每个命令成功即留下一条新事件 ----

    def register(self, case_id, actor, role, *, idempotency_key=None):
        """提交人登记新案例。"""
        return self._record(case_id, "registered", actor, role, {},
                            idempotency_key=idempotency_key)

    def submit(self, case_id, actor, role, *, expected_version=None, idempotency_key=None):
        """提交人提交案例，进入脱敏复核。"""
        return self._record(case_id, "submitted", actor, role, {},
                            expected_version, idempotency_key)

    def return_case(self, case_id, actor, role, *, reason="", expected_version=None,
                    idempotency_key=None):
        """脱敏复核人退回案例，原因随事件留存。"""
        return self._record(case_id, "returned", actor, role, {"reason": reason},
                            expected_version, idempotency_key)

    def approve(self, case_id, actor, role, *, expected_version=None, idempotency_key=None):
        """脱敏复核人确认脱敏通过。"""
        return self._record(case_id, "approved", actor, role, {},
                            expected_version, idempotency_key)

    def publish(self, case_id, actor, role, *, expected_version=None, idempotency_key=None):
        """发布人发布案例；同一案例全库最多只能有一条发布事件。"""
        return self._record(case_id, "published", actor, role, {},
                            expected_version, idempotency_key)

    # ---- 查询 ----

    def case(self, case_id):
        """案例当前状态投影。"""
        with self._lock:
            view = self._cases.get(case_id)
            if view is None:
                raise UnknownCase("unknown case: %s" % case_id)
            return {"id": case_id, **view}

    def snapshot(self):
        """全部案例的当前状态投影。"""
        with self._lock:
            return [{"id": cid, **self._cases[cid]} for cid in sorted(self._cases)]

    def history(self, case_id):
        """审核入口：按版本顺序返回案例从登记到当前的全部决定记录。"""
        with self._lock:
            if case_id not in self._cases:
                raise UnknownCase("unknown case: %s" % case_id)
            return self._store.events_of(case_id)

    # ---- 内部 ----

    def _record(self, case_id, action, actor, role, payload,
                expected_version=None, idempotency_key=None):
        required_role, allowed_from, _ = ACTIONS[action]
        with self._lock:
            if idempotency_key is not None:  # 客户端重试：同一键只记录一次
                if self._store.find_by_idempotency_key(case_id, idempotency_key) is not None:
                    return {"id": case_id, **self._cases[case_id]}
            view = self._cases.get(case_id)
            if allowed_from is None:
                if view is not None:
                    raise DuplicateCase("duplicate case: %s" % case_id)
                version = 0
            else:
                if view is None:
                    raise UnknownCase("unknown case: %s" % case_id)
                version = view["version"]
            if role != required_role:
                raise RoleNotAllowed("%s requires role %s, got %s" % (action, required_role, role))
            if allowed_from is not None and view["state"] not in allowed_from:
                raise InvalidTransition("cannot %s from state %s" % (action, view["state"]))
            if expected_version is not None and expected_version != version:
                raise VersionConflict("expected version %s, current %s" % (expected_version, version))
            event = {
                "case_id": case_id,
                "version": version + 1,
                "type": action,
                "actor": actor,
                "role": role,
                "payload": payload,
                "idempotency_key": idempotency_key,
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
            try:
                self._store.append(event)
            except sqlite3.IntegrityError as exc:
                # 并发下同一版本号 / 同一次发布 / 同一幂等键只能有一个赢家
                raise VersionConflict(str(exc)) from exc
            self._apply(event)
            return {"id": case_id, **self._cases[case_id]}

    def _apply(self, event):
        self._cases[event["case_id"]] = {
            "state": ACTIONS[event["type"]][2],
            "version": event["version"],
        }
