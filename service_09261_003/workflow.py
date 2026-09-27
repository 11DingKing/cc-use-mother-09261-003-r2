"""版本化业务工作流：角色分明的脱敏交付流程。

三类角色各司其职，每次决定都以"新事件"的形式追加，旧决定永不被覆盖：

    提交人 submitter : 建档 / 提交 / 退回后修改再提交
    复核人 reviewer  : 复核通过 / 退回（必须写明原因）
    发布人 publisher : 将复核通过的案例发布给外部教师

状态流转：
    draft ──submit──▶ submitted ──approve──▶ approved ──publish──▶ published
                          │
                       return
                          ▼
                       returned ──submit──▶ submitted（重新进入复核）

工作流本身不保存任何可变状态；当前状态一律通过重放 store 中的事件得到，
因此进程重启后历史完整保留。
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Callable

from .store import ConcurrencyConflict, SQLiteEventStore

ROLE_SUBMITTER = "submitter"
ROLE_REVIEWER = "reviewer"
ROLE_PUBLISHER = "publisher"
ROLES = (ROLE_SUBMITTER, ROLE_REVIEWER, ROLE_PUBLISHER)

# 事件类型
EV_CREATED = "CaseCreated"
EV_SUBMITTED = "Submitted"
EV_RETURNED = "Returned"
EV_APPROVED = "ReviewApproved"
EV_PUBLISHED = "Published"

# 每个动作允许的前置状态；None 表示不依赖已有案例（建档）
TRANSITIONS: dict[str, tuple[str, ...]] = {
    EV_SUBMITTED: ("draft", "returned"),
    EV_RETURNED: ("submitted",),
    EV_APPROVED: ("submitted",),
    EV_PUBLISHED: ("approved",),
}
EVENT_ROLE = {
    EV_CREATED: ROLE_SUBMITTER,
    EV_SUBMITTED: ROLE_SUBMITTER,
    EV_RETURNED: ROLE_REVIEWER,
    EV_APPROVED: ROLE_REVIEWER,
    EV_PUBLISHED: ROLE_PUBLISHER,
}
# 事件发生后案例所处的状态
EVENT_STATE = {
    EV_CREATED: "draft",
    EV_SUBMITTED: "submitted",
    EV_RETURNED: "returned",
    EV_APPROVED: "approved",
    EV_PUBLISHED: "published",
}


class WorkflowError(Exception):
    """业务规则违反基类。"""


class RoleError(WorkflowError):
    """操作者角色与该步骤要求的角色不符。"""


class StateError(WorkflowError):
    """案例当前状态不允许该动作。"""


class NotFoundError(WorkflowError):
    """案例不存在。"""


def fingerprint(content: Any) -> str:
    """对案例内容计算稳定指纹，用于审计时核对当时提交的到底是什么。"""
    raw = json.dumps(content, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class CaseView:
    """案例在某个时间点的只读视图。"""

    case_id: str
    state: str
    version: int
    last_actor: str
    last_event: str

    def to_dict(self) -> dict:
        return {
            "id": self.case_id,
            "state": self.state,
            "version": self.version,
            "last_actor": self.last_actor,
            "last_event": self.last_event,
        }


class Workflow:
    def __init__(self, store: SQLiteEventStore | None = None):
        self.store = store or SQLiteEventStore()

    # ---------- 对外命令 ----------

    def create_case(
        self,
        case_id: str,
        actor: str,
        role: str = ROLE_SUBMITTER,
        content: Any = None,
        idempotency_key: str | None = None,
    ) -> dict:
        """提交人建立脱敏案例草稿（version 1）。"""
        self._require_role(EV_CREATED, role)
        payload = {"content": content, "content_fingerprint": fingerprint(content)}
        try:
            return self.store.append(
                case_id, EV_CREATED, actor, role, payload,
                expected_version=0, idempotency_key=idempotency_key,
            )
        except ConcurrencyConflict:
            # version 0 不匹配意味着同 ID 案例已被（并发）建档
            raise StateError(f"案例 {case_id} 已存在，不能重复建档")

    def submit(
        self,
        case_id: str,
        actor: str,
        role: str = ROLE_SUBMITTER,
        content: Any = None,
        comment: str | None = None,
        expected_version: int | None = None,
        idempotency_key: str | None = None,
    ) -> dict:
        """提交人把案例送审；退回后允许修改内容重新提交（留下新记录）。"""
        return self._decide(
            EV_SUBMITTED, case_id, actor, role,
            payload_builder=lambda prev: {
                "content": content if content is not None else prev.get("content"),
                "content_fingerprint": fingerprint(
                    content if content is not None else prev.get("content")
                ),
                "comment": comment,
            },
            expected_version=expected_version,
            idempotency_key=idempotency_key,
        )

    def return_case(
        self,
        case_id: str,
        actor: str,
        reason: str,
        role: str = ROLE_REVIEWER,
        expected_version: int | None = None,
        idempotency_key: str | None = None,
    ) -> dict:
        """复核人退回案例，必须写明退回原因。"""
        if not reason or not str(reason).strip():
            raise WorkflowError("退回必须填写原因")
        return self._decide(
            EV_RETURNED, case_id, actor, role,
            payload_builder=lambda prev: {"reason": reason},
            expected_version=expected_version,
            idempotency_key=idempotency_key,
        )

    def approve(
        self,
        case_id: str,
        actor: str,
        role: str = ROLE_REVIEWER,
        comment: str | None = None,
        expected_version: int | None = None,
        idempotency_key: str | None = None,
    ) -> dict:
        """复核人复核通过。"""
        return self._decide(
            EV_APPROVED, case_id, actor, role,
            payload_builder=lambda prev: {"comment": comment},
            expected_version=expected_version,
            idempotency_key=idempotency_key,
        )

    def publish(
        self,
        case_id: str,
        actor: str,
        role: str = ROLE_PUBLISHER,
        comment: str | None = None,
        expected_version: int | None = None,
        idempotency_key: str | None = None,
    ) -> dict:
        """发布人发布案例；并发发布只有一个能成功。"""
        return self._decide(
            EV_PUBLISHED, case_id, actor, role,
            payload_builder=lambda prev: {"comment": comment},
            expected_version=expected_version,
            idempotency_key=idempotency_key,
        )

    # ---------- 查询 / 审计 ----------

    def get_case(self, case_id: str) -> CaseView:
        events = self.store.events(case_id)
        if not events:
            raise NotFoundError(f"案例 {case_id} 不存在")
        last = events[-1]
        return CaseView(
            case_id=case_id,
            state=EVENT_STATE[last["type"]],
            version=last["version"],
            last_actor=last["actor"],
            last_event=last["type"],
        )

    def list_cases(self) -> list[dict]:
        latest: dict[str, dict] = {}
        for event in self.store.all_events():
            latest[event["case_id"]] = event
        return [
            CaseView(
                case_id=case_id,
                state=EVENT_STATE[event["type"]],
                version=event["version"],
                last_actor=event["actor"],
                last_event=event["type"],
            ).to_dict()
            for case_id, event in sorted(latest.items())
        ]

    def history(self, case_id: str) -> dict:
        """还原完整决策过程：每个决定一条不可变记录。"""
        events = self.store.events(case_id)
        if not events:
            raise NotFoundError(f"案例 {case_id} 不存在")
        return {
            "case_id": case_id,
            "current_state": EVENT_STATE[events[-1]["type"]],
            "current_version": events[-1]["version"],
            "decisions": [self._decision_view(e) for e in events],
        }

    # ---------- 内部机制 ----------

    def _decide(
        self,
        event_type: str,
        case_id: str,
        actor: str,
        role: str,
        *,
        payload_builder: Callable[[dict], dict],
        expected_version: int | None,
        idempotency_key: str | None,
    ) -> dict:
        # 幂等重放优先于一切校验：客户端超时重试时，哪怕案例状态已经向前推进，
        # 也必须原样返回首次决定，而不是报状态错误或产生第二条记录。
        if idempotency_key is not None:
            replayed = self.store.find_by_idempotency_key(idempotency_key)
            if replayed is not None:
                return replayed, True

        self._require_role(event_type, role)
        events = self.store.events(case_id)
        if not events:
            raise NotFoundError(f"案例 {case_id} 不存在")
        current = events[-1]
        # 乐观锁优先：请求所基于的版本若已被他人推进，无论状态机是否允许，
        # 都按并发冲突处理，迫使调用方基于最新事实重新决定。
        if expected_version is not None and expected_version != current["version"]:
            raise ConcurrencyConflict(
                f"案例 {case_id} 已被他人推进到版本 {current['version']}"
                f"（{current['type']}），请基于最新版本重试"
            )
        current_state = EVENT_STATE[current["type"]]
        if current_state not in TRANSITIONS[event_type]:
            raise StateError(
                f"案例当前为 {current_state} 状态，不能执行 {event_type}"
            )
        if expected_version is None:
            expected_version = current["version"]
        payload = payload_builder(current["payload"])
        try:
            return self.store.append(
                case_id, event_type, actor, role, payload,
                expected_version=expected_version,
                idempotency_key=idempotency_key,
            )
        except ConcurrencyConflict:
            # 读状态与追加之间被其他事务抢先（并发窗口），转换为最新事实提示
            latest = self.store.events(case_id)[-1]
            raise ConcurrencyConflict(
                f"案例 {case_id} 已被他人推进到版本 {latest['version']}"
                f"（{latest['type']}），请基于最新版本重试"
            )

    @staticmethod
    def _require_role(event_type: str, role: str) -> None:
        required = EVENT_ROLE[event_type]
        if role != required:
            raise RoleError(
                f"{event_type} 只允许 {required} 角色执行，当前角色为 {role}"
            )

    @staticmethod
    def _decision_view(event: dict) -> dict:
        return {
            "seq": event["seq"],
            "version": event["version"],
            "action": event["type"],
            "resulting_state": EVENT_STATE[event["type"]],
            "actor": event["actor"],
            "role": event["role"],
            "payload": event["payload"],
            "decided_at": event["created_at"],
            "idempotency_key": event["idempotency_key"],
        }
