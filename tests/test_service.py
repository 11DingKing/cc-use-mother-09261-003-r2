import os
import sqlite3
import tempfile
import threading
import unittest

from service_09261_003.api import dispatch
from service_09261_003.store import SQLiteStore
from service_09261_003.workflow import (
    DuplicateCase,
    InvalidTransition,
    Role,
    RoleNotAllowed,
    State,
    UnknownCase,
    VersionConflict,
    Workflow,
)


def make_flow(path=":memory:"):
    return Workflow(SQLiteStore(path))


class LifecycleTest(unittest.TestCase):
    def setUp(self):
        self.flow = make_flow()
        self.flow.register("c1", "alice", Role.SUBMITTER)

    def test_every_decision_appends_a_new_event(self):
        self.flow.submit("c1", "alice", Role.SUBMITTER)
        self.flow.return_case("c1", "bob", Role.REVIEWER, reason="含真实姓名")
        self.flow.submit("c1", "alice", Role.SUBMITTER)
        self.flow.approve("c1", "bob", Role.REVIEWER)
        view = self.flow.publish("c1", "carol", Role.PUBLISHER)
        self.assertEqual(view, {"id": "c1", "state": State.PUBLISHED, "version": 6})
        history = self.flow.history("c1")
        self.assertEqual([e["type"] for e in history],
                         ["registered", "submitted", "returned",
                          "submitted", "approved", "published"])
        self.assertEqual([e["version"] for e in history], [1, 2, 3, 4, 5, 6])
        # 退回决定及其原因没有被后来的重新提交覆盖，谁、以什么角色、何时做的都还在
        returned = history[2]
        self.assertEqual(returned["actor"], "bob")
        self.assertEqual(returned["role"], Role.REVIEWER)
        self.assertEqual(returned["payload"], {"reason": "含真实姓名"})
        self.assertTrue(all(e["created_at"] for e in history))

    def test_roles_are_enforced(self):
        with self.assertRaises(RoleNotAllowed):
            self.flow.submit("c1", "bob", Role.REVIEWER)
        self.flow.submit("c1", "alice", Role.SUBMITTER)
        with self.assertRaises(RoleNotAllowed):
            self.flow.approve("c1", "alice", Role.SUBMITTER)
        with self.assertRaises(RoleNotAllowed):
            self.flow.return_case("c1", "carol", Role.PUBLISHER)
        self.flow.approve("c1", "bob", Role.REVIEWER)
        with self.assertRaises(RoleNotAllowed):
            self.flow.publish("c1", "bob", Role.REVIEWER)
        # 越权尝试不会留下任何记录
        self.assertEqual([e["type"] for e in self.flow.history("c1")],
                         ["registered", "submitted", "approved"])

    def test_invalid_transition_and_unknown_case_rejected(self):
        with self.assertRaises(InvalidTransition):
            self.flow.approve("c1", "bob", Role.REVIEWER)
        with self.assertRaises(DuplicateCase):
            self.flow.register("c1", "alice", Role.SUBMITTER)
        with self.assertRaises(UnknownCase):
            self.flow.submit("ghost", "alice", Role.SUBMITTER)
        with self.assertRaises(UnknownCase):
            self.flow.history("ghost")

    def test_published_is_terminal(self):
        self.flow.submit("c1", "alice", Role.SUBMITTER)
        self.flow.approve("c1", "bob", Role.REVIEWER)
        self.flow.publish("c1", "carol", Role.PUBLISHER)
        with self.assertRaises(InvalidTransition):
            self.flow.publish("c1", "carol", Role.PUBLISHER)
        with self.assertRaises(InvalidTransition):
            self.flow.submit("c1", "alice", Role.SUBMITTER)

    def test_expected_version_optimistic_lock(self):
        self.flow.submit("c1", "alice", Role.SUBMITTER)  # version 变为 2
        with self.assertRaises(VersionConflict):
            self.flow.approve("c1", "bob", Role.REVIEWER, expected_version=1)
        self.flow.approve("c1", "bob", Role.REVIEWER, expected_version=2)
        self.assertEqual(self.flow.case("c1")["version"], 3)

    def test_idempotent_retry_records_once(self):
        first = self.flow.submit("c1", "alice", Role.SUBMITTER, idempotency_key="k1")
        again = self.flow.submit("c1", "alice", Role.SUBMITTER, idempotency_key="k1")
        self.assertEqual(first, again)
        self.assertEqual(len(self.flow.history("c1")), 2)  # registered + submitted


class ConcurrencyTest(unittest.TestCase):
    def test_concurrent_publish_has_exactly_one_winner(self):
        flow = make_flow()
        flow.register("c1", "alice", Role.SUBMITTER)
        flow.submit("c1", "alice", Role.SUBMITTER)
        flow.approve("c1", "bob", Role.REVIEWER)
        version = flow.case("c1")["version"]
        outcomes = []

        def publish(name):
            try:
                flow.publish("c1", name, Role.PUBLISHER, expected_version=version)
                outcomes.append("ok")
            except (VersionConflict, InvalidTransition):
                outcomes.append("lost")

        threads = [threading.Thread(target=publish, args=("publisher-%d" % i,))
                   for i in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(outcomes.count("ok"), 1)
        published = [e for e in flow.history("c1") if e["type"] == "published"]
        self.assertEqual(len(published), 1)  # 不存在两个互相冲突的发布结果

    def test_two_service_instances_cannot_double_publish(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "events.db")
            flow_a = make_flow(path)
            flow_a.register("c1", "alice", Role.SUBMITTER)
            flow_a.submit("c1", "alice", Role.SUBMITTER)
            flow_a.approve("c1", "bob", Role.REVIEWER)
            flow_b = make_flow(path)  # 另一个服务实例，共享同一事件库
            flow_a.publish("c1", "carol", Role.PUBLISHER)
            # flow_b 的投影已过期，但数据库唯一约束仍然挡住第二次发布
            with self.assertRaises((VersionConflict, InvalidTransition)):
                flow_b.publish("c1", "dave", Role.PUBLISHER)
            published = [e for e in flow_a.history("c1") if e["type"] == "published"]
            self.assertEqual(len(published), 1)


class PersistenceTest(unittest.TestCase):
    def test_history_survives_restart(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "events.db")
            flow = make_flow(path)
            flow.register("c1", "alice", Role.SUBMITTER)
            flow.submit("c1", "alice", Role.SUBMITTER)
            flow.return_case("c1", "bob", Role.REVIEWER, reason="需二次脱敏")
            before = flow.history("c1")
            reopened = make_flow(path)  # 模拟重启：全新对象，同一数据文件
            self.assertEqual(reopened.history("c1"), before)
            self.assertEqual(reopened.case("c1")["state"], State.RETURNED)
            reopened.submit("c1", "alice", Role.SUBMITTER)  # 重启后版本继续递增
            self.assertEqual(reopened.case("c1")["version"], 4)

    def test_store_is_append_only(self):
        store = SQLiteStore()
        flow = Workflow(store)
        flow.register("c1", "alice", Role.SUBMITTER)
        with self.assertRaises(sqlite3.IntegrityError):
            store.db.execute("UPDATE events SET actor = 'mallory'")
        with self.assertRaises(sqlite3.IntegrityError):
            store.db.execute("DELETE FROM events")
        store.db.rollback()
        self.assertEqual(len(flow.history("c1")), 1)


class ApiTest(unittest.TestCase):
    def setUp(self):
        self.flow = make_flow()

    def test_http_flow_and_audit_endpoint(self):
        status, created = dispatch(self.flow, "POST", "/cases",
                                   {"id": "c1", "actor": "alice", "role": "submitter"})
        self.assertEqual((status, created["state"]), (201, "draft"))
        status, _ = dispatch(self.flow, "POST", "/cases/c1/submit",
                             {"actor": "alice", "role": "submitter"})
        self.assertEqual(status, 200)
        status, err = dispatch(self.flow, "POST", "/cases/c1/approve",
                               {"actor": "alice", "role": "submitter"})
        self.assertEqual((status, err["error"]), (403, "role_not_allowed"))
        status, err = dispatch(self.flow, "POST", "/cases/c1/publish",
                               {"actor": "carol", "role": "publisher"})
        self.assertEqual((status, err["error"]), (409, "invalid_transition"))
        dispatch(self.flow, "POST", "/cases/c1/return",
                 {"actor": "bob", "role": "reviewer", "reason": "含真实工号"})
        dispatch(self.flow, "POST", "/cases/c1/submit",
                 {"actor": "alice", "role": "submitter"})
        dispatch(self.flow, "POST", "/cases/c1/approve", {"actor": "bob", "role": "reviewer"})
        status, _ = dispatch(self.flow, "POST", "/cases/c1/publish",
                             {"actor": "carol", "role": "publisher"})
        self.assertEqual(status, 200)
        # 审核人员通过接口还原完整过程
        status, history = dispatch(self.flow, "GET", "/cases/c1/history")
        self.assertEqual(status, 200)
        self.assertEqual([e["type"] for e in history],
                         ["registered", "submitted", "returned",
                          "submitted", "approved", "published"])
        self.assertEqual(history[2]["payload"]["reason"], "含真实工号")
        status, view = dispatch(self.flow, "GET", "/cases/c1")
        self.assertEqual((status, view["state"]), (200, "published"))
        status, listing = dispatch(self.flow, "GET", "/cases")
        self.assertEqual((status, len(listing)), (200, 1))

    def test_api_error_mapping(self):
        self.assertEqual(dispatch(self.flow, "GET", "/cases/ghost")[0], 404)
        self.assertEqual(dispatch(self.flow, "GET", "/cases/ghost/history")[0], 404)
        self.assertEqual(dispatch(self.flow, "POST", "/cases", {"id": "c1"})[0], 400)
        self.assertEqual(dispatch(self.flow, "GET", "/nope")[0], 404)
        self.assertEqual(dispatch(self.flow, "DELETE", "/cases")[0], 404)


if __name__ == "__main__":
    unittest.main()
