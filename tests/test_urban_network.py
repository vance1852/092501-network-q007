import os, tempfile, threading, unittest
from urban_network.errors import CapacityExceeded, Conflict
from urban_network.models import Reading,Segment
from urban_network.risk import score_reading
from urban_network.service import NetworkService
class UrbanNetworkTests(unittest.TestCase):
    def setUp(self):
        self.s=NetworkService(); self.s.bootstrap(); self.t=self.s.auth.login("admin","network-admin"); self.s.register_segment(self.t,Segment("S1","east","drainage",100,4))
    def _open_order(self,reading_id="R2",resource_id="P1",capacity=1):
        alert=self.s.ingest_reading(self.t,Reading(reading_id,"S1","sensor",100,250,90,"2026-01-01T00:00:00+00:00"))["alert_id"]
        order=self.s.create_work_order(self.t,"S1",alert,"crew")["work_order_id"]
        self.s.add_resource(self.t,resource_id,"pump","east",capacity)
        return resource_id,order
    def test_risk_and_idempotent_reading(self):
        r=Reading("R1","S1","sensor",120,250,90,"2026-01-01T00:00:00+00:00"); a=self.s.ingest_reading(self.t,r); b=self.s.ingest_reading(self.t,r); self.assertFalse(a["duplicate"]); self.assertTrue(b["duplicate"]); self.assertEqual(self.s.risk_report(self.t,"S1")["readings"],1)
    def test_work_order_and_allocation(self):
        r=self.s.ingest_reading(self.t,Reading("R2","S1","sensor",100,250,90,"2026-01-01T00:00:00+00:00")); o=self.s.create_work_order(self.t,"S1",r["alert_id"],"crew"); self.s.transition_work_order(self.t,o["work_order_id"],"assigned","crew accepted"); self.s.add_resource(self.t,"R1","pump","east",2); self.assertFalse(self.s.allocate(self.t,"R1",o["work_order_id"],1)["duplicate"]); self.assertEqual(self.s.resource(self.t,"R1")["available"],1)
    def test_retry_replays_when_balance_is_zero(self):
        rid,oid=self._open_order(capacity=1)
        first=self.s.allocate(self.t,rid,oid,1); self.assertFalse(first["duplicate"])
        self.assertEqual(self.s.resource(self.t,rid)["available"],0)
        # 重试成功过的分配：余额已为零也必须重放原结果，而不是报容量不足。
        replay=self.s.allocate(self.t,rid,oid,1)
        self.assertTrue(replay["duplicate"]); self.assertEqual(replay["allocation_id"],first["allocation_id"]); self.assertEqual(replay["quantity"],1)
        self.assertEqual(self.s.resource(self.t,rid)["available"],0)
        allocations=self.s.db.execute("SELECT COUNT(*) c FROM allocations").fetchone()["c"]; self.assertEqual(allocations,1)
    def test_same_pair_different_quantity_conflicts(self):
        rid,oid=self._open_order(capacity=5)
        first=self.s.allocate(self.t,rid,oid,2); self.assertFalse(first["duplicate"])
        with self.assertRaises(Conflict):self.s.allocate(self.t,rid,oid,3)
        # 冲突不得二次扣减或新增记录。
        self.assertEqual(self.s.resource(self.t,rid)["available"],3)
        self.assertEqual(self.s.db.execute("SELECT COUNT(*) c FROM allocations").fetchone()["c"],1)
        # 原数量仍可重放。
        replay=self.s.allocate(self.t,rid,oid,2); self.assertTrue(replay["duplicate"]); self.assertEqual(replay["allocation_id"],first["allocation_id"])
    def test_capacity_checked_only_for_new_allocation(self):
        rid,oid=self._open_order(capacity=1)
        self.s.allocate(self.t,rid,oid,1)
        # 新工单的新分配在容量不足时才报错。
        other=self.s.create_work_order(self.t,"S1",self.s.ingest_reading(self.t,Reading("R9","S1","sensor",100,250,90,"2026-01-02T00:00:00+00:00"))["alert_id"],"crew")["work_order_id"]
        with self.assertRaises(CapacityExceeded):self.s.allocate(self.t,rid,other,1)
        # 失败的新分配必须完整回滚：无分配记录、无审计、余额不变。
        self.assertEqual(self.s.resource(self.t,rid)["available"],0)
        self.assertEqual(self.s.db.execute("SELECT COUNT(*) c FROM allocations").fetchone()["c"],1)
        self.assertEqual(len(self.s.audit_events(self.t,"resource",rid)),2)
    def test_concurrent_first_submits_create_one_allocation(self):
        path=tempfile.mktemp(suffix=".db")
        try:
            seed=NetworkService(path); seed.bootstrap(); tok=seed.auth.login("admin","network-admin")
            seed.register_segment(tok,Segment("S1","east","drainage",100,4))
            alert=seed.ingest_reading(tok,Reading("R2","S1","sensor",100,250,90,"2026-01-01T00:00:00+00:00"))["alert_id"]
            oid=seed.create_work_order(tok,"S1",alert,"crew")["work_order_id"]
            seed.add_resource(tok,"P1","pump","east",1); seed.db.close()
            results=[]; errors=[]; lock=threading.Lock()
            def submit():
                svc=NetworkService(path); t=svc.auth.login("admin","network-admin")
                try:
                    r=svc.allocate(t,"P1",oid,1)
                    with lock:results.append(r)
                except Exception as exc:
                    with lock:errors.append(exc)
                finally:svc.db.close()
            threads=[threading.Thread(target=submit) for _ in range(8)]
            for th in threads:th.start()
            for th in threads:th.join()
            self.assertFalse(errors)
            ids={r["allocation_id"] for r in results}; self.assertEqual(len(ids),1)
            self.assertEqual(sum(0 if r["duplicate"] else 1 for r in results),1)
            verify=NetworkService(path); vt=verify.auth.login("admin","network-admin")
            self.assertEqual(verify.resource(vt,"P1")["available"],0)
            self.assertEqual(verify.db.execute("SELECT COUNT(*) c FROM allocations").fetchone()["c"],1)
            self.assertEqual(len(verify.audit_events(vt,"resource","P1")),2)
            verify.db.close()
        finally:
            for suffix in ("","-wal","-shm"):
                p=path+suffix
                if os.path.exists(p):os.remove(p)
    def test_state_consistent_after_restart(self):
        path=tempfile.mktemp(suffix=".db")
        try:
            first=NetworkService(path); first.bootstrap(); tok=first.auth.login("admin","network-admin")
            first.register_segment(tok,Segment("S1","east","drainage",100,4))
            alert=first.ingest_reading(tok,Reading("R2","S1","sensor",100,250,90,"2026-01-01T00:00:00+00:00"))["alert_id"]
            oid=first.create_work_order(tok,"S1",alert,"crew")["work_order_id"]
            first.add_resource(tok,"P1","pump","east",1)
            created=first.allocate(tok,"P1",oid,1); aid=created["allocation_id"]; first.db.close()
            # 进程重启后重放原分配，余额仍为零且不新增记录/审计。
            restarted=NetworkService(path); tok2=restarted.auth.login("admin","network-admin")
            replay=restarted.allocate(tok2,"P1",oid,1)
            self.assertTrue(replay["duplicate"]); self.assertEqual(replay["allocation_id"],aid)
            self.assertEqual(restarted.resource(tok2,"P1")["available"],0)
            self.assertEqual(restarted.db.execute("SELECT COUNT(*) c FROM allocations").fetchone()["c"],1)
            self.assertEqual(len(restarted.audit_events(tok2,"resource","P1")),2)
            restarted.db.close()
        finally:
            for suffix in ("","-wal","-shm"):
                p=path+suffix
                if os.path.exists(p):os.remove(p)
    def test_risk_validation(self):
        with self.assertRaises(ValueError):score_reading(-1,1,1,2)
