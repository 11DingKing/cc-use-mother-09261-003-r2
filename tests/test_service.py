import os
import sqlite3
import tempfile
import threading
import unittest

from service_09261_003 import (
    ConcurrencyConflict,
    ROLE_PUBLISHER,
    ROLE_REVIEWER,
    ROLE_SUBMITTER,
    RoleError,
    SQLiteEventStore,
    StateError,
    Workflow,
    WorkflowError,
)
from service_09261_003.api import dispatch


class HappyPathTest(unittest.TestCase):
    def setUp(self):
        self.flow = Workflow(SQLiteEventStore())

    def test_full_flow_with_distinct_roles(self):
        self.flow.create_case("c1", "alice", content={"title": "桥梁监测"})
        self.flow.submit("c1", "alice", comment="脱敏完成请复核")
        self.flow.approve("c1", "bob", comment="无残留敏感信息")
        self.flow.publish("c1", "carol")

        view = self.flow.get_case("c1")
        self.assertEqual(view.state, "published")
        self.assertEqual(view.version, 4)
        self.assertEqual(view.last_actor, "carol")

        history = self.flow.history("c1")
        self.assertEqual(history["current_state"], "published")
        actors = [(d["action"], d["actor"], d["role"]) for d in history["decisions"]]
        self.assertEqual(actors, [
            ("CaseCreated", "alice", ROLE_SUBMITTER),
            ("Submitted", "alice", ROLE_SUBMITTER),
            ("ReviewApproved", "bob", ROLE_REVIEWER),
            ("Published", "carol", ROLE_PUBLISHER),
        ])

    def test_return_then_resubmit_keeps_every_decision(self):
        self.flow.create_case("c2", "alice", content={"v": 1})
        self.flow.submit("c2", "alice")
        self.flow.return_case("c2", "bob", reason="第3页仍有真实单位名称")
        self.flow.submit("c2", "alice", content={"v": 2}, comment="已替换单位名称")
        self.flow.approve("c2", "bob")
        self.flow.publish("c2", "carol")

        decisions = self.flow.history("c2")["decisions"]
        self.assertEqual(len(decisions), 6)  # 建档、提交、退回、再提交、通过、发布
        # 旧决定原封不动：首次提交内容与退回原因都可还原
        self.assertEqual(decisions[1]["payload"]["content"], {"v": 1})
        self.assertEqual(decisions[2]["payload"]["reason"], "第3页仍有真实单位名称")
        self.assertEqual(decisions[3]["payload"]["content"], {"v": 2})
        # 指纹不同，能证明两次提交的是不同内容
        self.assertNotEqual(
            decisions[1]["payload"]["content_fingerprint"],
            decisions[3]["payload"]["content_fingerprint"],
        )
        states = [d["resulting_state"] for d in decisions]
        self.assertEqual(
            states, ["draft", "submitted", "returned", "submitted", "approved", "published"]
        )

    def test_return_requires_reason(self):
        self.flow.create_case("c3", "alice")
        self.flow.submit("c3", "alice")
        with self.assertRaises(WorkflowError):
            self.flow.return_case("c3", "bob", reason="   ")


class RoleEnforcementTest(unittest.TestCase):
    def setUp(self):
        self.flow = Workflow(SQLiteEventStore())
        self.flow.create_case("c1", "alice")

    def test_submitter_cannot_approve_or_publish(self):
        self.flow.submit("c1", "alice")
        with self.assertRaises(RoleError):
            self.flow.approve("c1", "alice", role=ROLE_SUBMITTER)
        with self.assertRaises(RoleError):
            self.flow.publish("c1", "alice", role=ROLE_SUBMITTER)

    def test_reviewer_cannot_submit(self):
        with self.assertRaises(RoleError):
            self.flow.submit("c1", "bob", role=ROLE_REVIEWER)

    def test_publisher_cannot_return(self):
        self.flow.submit("c1", "alice")
        with self.assertRaises(RoleError):
            self.flow.return_case("c1", "carol", reason="x", role=ROLE_PUBLISHER)


class StateGuardTest(unittest.TestCase):
    def setUp(self):
        self.flow = Workflow(SQLiteEventStore())
        self.flow.create_case("c1", "alice")

    def test_cannot_publish_before_approval(self):
        self.flow.submit("c1", "alice")
        with self.assertRaises(StateError):
            self.flow.publish("c1", "carol")

    def test_cannot_approve_a_draft(self):
        with self.assertRaises(StateError):
            self.flow.approve("c1", "bob")

    def test_cannot_publish_twice(self):
        self.flow.submit("c1", "alice")
        self.flow.approve("c1", "bob")
        self.flow.publish("c1", "carol")
        with self.assertRaises(StateError):
            self.flow.publish("c1", "dave")

    def test_unknown_case(self):
        from service_09261_003 import NotFoundError
        with self.assertRaises(NotFoundError):
            self.flow.submit("nope", "alice")


class AppendOnlyTest(unittest.TestCase):
    def test_events_table_rejects_update_and_delete(self):
        store = SQLiteEventStore()
        flow = Workflow(store)
        flow.create_case("c1", "alice")
        with self.assertRaises(sqlite3.Error):
            store.db.execute("UPDATE events SET actor = 'mallory' WHERE case_id = 'c1'")
        with self.assertRaises(sqlite3.Error):
            store.db.execute("DELETE FROM events WHERE case_id = 'c1'")
        store.db.rollback()
        # 触发器拦截后原始记录仍然完好
        self.assertEqual(flow.history("c1")["decisions"][0]["actor"], "alice")


class ConcurrencyTest(unittest.TestCase):
    def test_stale_expected_version_rejected(self):
        flow = Workflow(SQLiteEventStore())
        flow.create_case("c1", "alice")
        flow.submit("c1", "alice", expected_version=1)
        # 复核人基于 v2 通过；另一个请求仍拿着 v2 退回，必须被拒绝
        flow.approve("c1", "bob", expected_version=2)
        with self.assertRaises(ConcurrencyConflict):
            flow.return_case("c1", "bob2", reason="迟来的退回", expected_version=2)

    def test_concurrent_publish_only_one_wins(self):
        store = SQLiteEventStore()
        flow = Workflow(store)
        flow.create_case("c1", "alice")
        flow.submit("c1", "alice")
        flow.approve("c1", "bob")
        approved_version = flow.get_case("c1").version  # 3

        results = []

        def publish_as(actor):
            try:
                event, _ = flow.publish(
                    "c1", actor, expected_version=approved_version
                )
                results.append(("ok", actor, event["version"]))
            except ConcurrencyConflict:
                results.append(("conflict", actor, None))

        threads = [
            threading.Thread(target=publish_as, args=(a,))
            for a in ("carol", "dave", "erin")
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        wins = [r for r in results if r[0] == "ok"]
        self.assertEqual(len(wins), 1, f"应当只有一个发布成功，实际: {results}")
        self.assertEqual(len([r for r in results if r[0] == "conflict"]), 2)
        # 历史中确实只有一条 Published
        published = [
            d for d in flow.history("c1")["decisions"] if d["action"] == "Published"
        ]
        self.assertEqual(len(published), 1)
        self.assertEqual(published[0]["actor"], wins[0][1])

    def test_concurrent_create_same_id_only_one_wins(self):
        store = SQLiteEventStore()
        flow = Workflow(store)
        errors = []

        def create():
            try:
                flow.create_case("dup", "alice")
            except (StateError, ConcurrencyConflict):
                errors.append(1)

        threads = [threading.Thread(target=create) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(len(errors), 4)


class PersistenceTest(unittest.TestCase):
    def test_history_survives_restart(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "cases.db")
            store = SQLiteEventStore(path)
            flow = Workflow(store)
            flow.create_case("c1", "alice", content={"title": "旧桥改造"})
            flow.submit("c1", "alice")
            flow.return_case("c1", "bob", reason="坐标未脱敏")
            flow.submit("c1", "alice", content={"title": "某市政桥梁"})
            flow.approve("c1", "bob")
            flow.publish("c1", "carol")
            store.close()

            # 模拟服务重启：新连接、新工作流对象，重放事件还原全部过程
            store2 = SQLiteEventStore(path)
            flow2 = Workflow(store2)
            view = flow2.get_case("c1")
            self.assertEqual(view.state, "published")
            self.assertEqual(view.version, 6)
            decisions = flow2.history("c1")["decisions"]
            self.assertEqual(len(decisions), 6)
            self.assertEqual(decisions[2]["payload"]["reason"], "坐标未脱敏")
            self.assertEqual(decisions[0]["payload"]["content"], {"title": "旧桥改造"})
            self.assertEqual(decisions[3]["payload"]["content"], {"title": "某市政桥梁"})


class IdempotencyTest(unittest.TestCase):
    def test_same_key_returns_original_decision(self):
        flow = Workflow(SQLiteEventStore())
        e1, replayed1 = flow.create_case(
            "c1", "alice", content={"x": 1}, idempotency_key="k-1"
        )
        self.assertFalse(replayed1)
        e2, replayed2 = flow.create_case(
            "c1", "mallory", content={"x": 2}, idempotency_key="k-1"
        )
        self.assertTrue(replayed2)
        # 返回的是首次决定，重试者的内容和身份都没有生效
        self.assertEqual(e2["seq"], e1["seq"])
        self.assertEqual(e2["actor"], "alice")
        self.assertEqual(len(flow.history("c1")["decisions"]), 1)

    def test_idempotent_publish_retry_after_timeout(self):
        flow = Workflow(SQLiteEventStore())
        flow.create_case("c1", "alice")
        flow.submit("c1", "alice")
        flow.approve("c1", "bob")
        e1, r1 = flow.publish("c1", "carol", idempotency_key="pub-1")
        e2, r2 = flow.publish("c1", "carol", idempotency_key="pub-1")
        self.assertFalse(r1)
        self.assertTrue(r2)
        self.assertEqual(e1["seq"], e2["seq"])
        self.assertEqual(
            len([d for d in flow.history("c1")["decisions"] if d["action"] == "Published"]),
            1,
        )


class ApiTest(unittest.TestCase):
    def setUp(self):
        self.flow = Workflow(SQLiteEventStore())

    def req(self, method, path, body=None):
        return dispatch(self.flow, method, path, body)

    def test_status_codes_full_flow(self):
        s, b = self.req("POST", "/cases",
                        {"id": "c1", "actor": "alice", "content": {"a": 1}})
        self.assertEqual(s, 201)
        self.assertFalse(b["idempotent_replay"])

        s, _ = self.req("POST", "/cases/c1/submit", {"actor": "alice"})
        self.assertEqual(s, 201)
        s, _ = self.req("POST", "/cases/c1/approve", {"actor": "bob"})
        self.assertEqual(s, 201)
        s, _ = self.req("POST", "/cases/c1/publish", {"actor": "carol"})
        self.assertEqual(s, 201)

        s, body = self.req("GET", "/cases/c1/history")
        self.assertEqual(s, 200)
        self.assertEqual(len(body["decisions"]), 4)

        s, body = self.req("GET", "/cases")
        self.assertEqual(s, 200)
        self.assertEqual(body["cases"][0]["state"], "published")

    def test_role_violation_is_403(self):
        self.req("POST", "/cases", {"id": "c1", "actor": "alice"})
        self.req("POST", "/cases/c1/submit", {"actor": "alice"})
        s, body = self.req("POST", "/cases/c1/approve",
                           {"actor": "alice", "role": ROLE_SUBMITTER})
        self.assertEqual(s, 403)
        self.assertEqual(body["error"], "forbidden_role")

    def test_concurrent_conflict_is_409(self):
        self.req("POST", "/cases", {"id": "c1", "actor": "alice"})
        self.req("POST", "/cases/c1/submit",
                 {"actor": "alice", "expected_version": 1})
        self.req("POST", "/cases/c1/approve",
                 {"actor": "bob", "expected_version": 2})
        s, body = self.req("POST", "/cases/c1/return",
                           {"actor": "bob2", "reason": "晚到", "expected_version": 2})
        self.assertEqual(s, 409)
        self.assertEqual(body["error"], "concurrency_conflict")

    def test_bad_state_and_missing_reason_are_422(self):
        self.req("POST", "/cases", {"id": "c1", "actor": "alice"})
        s, _ = self.req("POST", "/cases/c1/publish", {"actor": "carol"})
        self.assertEqual(s, 422)
        self.req("POST", "/cases/c1/submit", {"actor": "alice"})
        s, body = self.req("POST", "/cases/c1/return", {"actor": "bob"})
        self.assertEqual(s, 422)
        self.assertIn("原因", body["message"])

    def test_unknown_case_is_404_and_idempotent_replay_is_200(self):
        s, _ = self.req("GET", "/cases/nope/history")
        self.assertEqual(s, 404)

        s1, _ = self.req("POST", "/cases",
                         {"id": "c2", "actor": "alice", "idempotency_key": "k"})
        s2, body = self.req("POST", "/cases",
                            {"id": "c2", "actor": "alice", "idempotency_key": "k"})
        self.assertEqual(s1, 201)
        self.assertEqual(s2, 200)
        self.assertTrue(body["idempotent_replay"])


if __name__ == "__main__":
    unittest.main()
