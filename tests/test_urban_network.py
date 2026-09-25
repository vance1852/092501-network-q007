import os,tempfile,threading,unittest
from urban_network.models import Reading,Segment
from urban_network.risk import score_reading
from urban_network.service import NetworkService
class UrbanNetworkTests(unittest.TestCase):
    def setUp(self):
        self.s=NetworkService(); self.s.bootstrap(); self.t=self.s.auth.login("admin","network-admin"); self.s.register_segment(self.t,Segment("S1","east","drainage",100,4))
    def _order(self,reading_id):
        r=self.s.ingest_reading(self.t,Reading(reading_id,"S1",reading_id,100,250,90,"2026-01-01T00:00:00+00:00")); return self.s.create_work_order(self.t,"S1",r["alert_id"],"crew")
    def test_risk_and_idempotent_reading(self):
        r=Reading("R1","S1","sensor",120,250,90,"2026-01-01T00:00:00+00:00"); a=self.s.ingest_reading(self.t,r); b=self.s.ingest_reading(self.t,r); self.assertFalse(a["duplicate"]); self.assertTrue(b["duplicate"]); self.assertEqual(self.s.risk_report(self.t,"S1")["readings"],1)
    def test_work_order_and_allocation(self):
        r=self.s.ingest_reading(self.t,Reading("R2","S1","sensor",100,250,90,"2026-01-01T00:00:00+00:00")); o=self.s.create_work_order(self.t,"S1",r["alert_id"],"crew"); self.s.transition_work_order(self.t,o["work_order_id"],"assigned","crew accepted"); self.s.add_resource(self.t,"R1","pump","east",2); self.assertFalse(self.s.allocate(self.t,"R1",o["work_order_id"],1)["duplicate"]); self.assertEqual(self.s.resource(self.t,"R1")["available"],1)
    def test_risk_validation(self):
        with self.assertRaises(ValueError):score_reading(-1,1,1,2)
    def test_allocation_replays_when_capacity_exhausted(self):
        o=self._order("R3"); self.s.add_resource(self.t,"RP","pump","east",2); a=self.s.allocate(self.t,"RP",o["work_order_id"],2); self.assertEqual(self.s.resource(self.t,"RP")["available"],0)
        b=self.s.allocate(self.t,"RP",o["work_order_id"],2)
        self.assertTrue(b["duplicate"]); self.assertEqual(b["allocation_id"],a["allocation_id"]); self.assertEqual(b["resource_id"],"RP"); self.assertEqual(b["quantity"],2)
        self.assertEqual(self.s.db.execute("SELECT COUNT(*) FROM allocations").fetchone()[0],1); self.assertEqual(self.s.resource(self.t,"RP")["available"],0)
        self.assertEqual(len([e for e in self.s.audit_events(self.t,"resource","RP") if e["action"]=="allocated"]),1)
    def test_allocation_quantity_conflict(self):
        o=self._order("R4"); self.s.add_resource(self.t,"RC","pump","east",5); self.s.allocate(self.t,"RC",o["work_order_id"],2)
        with self.assertRaises(ValueError): self.s.allocate(self.t,"RC",o["work_order_id"],3)
        self.assertEqual(self.s.resource(self.t,"RC")["available"],3); self.assertEqual(self.s.db.execute("SELECT COUNT(*) FROM allocations").fetchone()[0],1)
        self.assertEqual(len([e for e in self.s.audit_events(self.t,"resource","RC") if e["action"]=="allocated"]),1)
    def test_allocation_capacity_checked_only_for_new(self):
        o1=self._order("R5"); o2=self._order("R6"); self.s.add_resource(self.t,"RL","pump","east",1); self.s.allocate(self.t,"RL",o1["work_order_id"],1)
        with self.assertRaises(ValueError): self.s.allocate(self.t,"RL",o2["work_order_id"],1)
        self.assertEqual(self.s.resource(self.t,"RL")["available"],0); self.assertEqual(self.s.db.execute("SELECT COUNT(*) FROM allocations").fetchone()[0],1)
        self.assertEqual(len([e for e in self.s.audit_events(self.t,"resource","RL") if e["action"]=="allocated"]),1)
    def test_concurrent_allocation_and_restart_replay(self):
        with tempfile.TemporaryDirectory() as d:
            path=os.path.join(d,"net.sqlite3"); s1=NetworkService(path); s1.bootstrap(); t1=s1.auth.login("admin","network-admin")
            s1.register_segment(t1,Segment("S9","east","drainage",100,4)); r=s1.ingest_reading(t1,Reading("R7","S9","sensor",100,250,90,"2026-01-01T00:00:00+00:00")); o=s1.create_work_order(t1,"S9",r["alert_id"],"crew"); s1.add_resource(t1,"RR","pump","east",1)
            results,errors=[],[]
            def call():
                try:
                    svc=NetworkService(path); tok=svc.auth.login("admin","network-admin"); results.append(svc.allocate(tok,"RR",o["work_order_id"],1))
                except Exception as e: errors.append(e)
            threads=[threading.Thread(target=call) for _ in range(2)]; [t.start() for t in threads]; [t.join() for t in threads]
            self.assertEqual(errors,[]); self.assertEqual(len(results),2); self.assertEqual(len({r["allocation_id"] for r in results}),1); self.assertEqual(sorted(r["duplicate"] for r in results),[False,True])
            s3=NetworkService(path); t3=s3.auth.login("admin","network-admin"); replay=s3.allocate(t3,"RR",o["work_order_id"],1)
            self.assertTrue(replay["duplicate"]); self.assertEqual(replay["allocation_id"],results[0]["allocation_id"])
            self.assertEqual(s3.resource(t3,"RR")["available"],0); self.assertEqual(s3.db.execute("SELECT COUNT(*) FROM allocations").fetchone()[0],1)
            self.assertEqual(len([e for e in s3.audit_events(t3,"resource","RR") if e["action"]=="allocated"]),1)
