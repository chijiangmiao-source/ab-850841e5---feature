#!/usr/bin/env python3
"""验收脚本：在页面与健康响应可用后运行。

覆盖内容（全部走真实 HTTP 接口）：
  A. 代码测试（unittest）
     - 同站同摘要同稳定投票标识的幂等重传：回放首次结果、不增票；
     - 投票标识内容改变 / 摘要不同：冲突票隔离、不计入赞成票、不参与封存；
     - 配置不符与参数无效的可操作拒绝（错误码可读）；
     - 审查员隔离裁决：既有赞成票排出有效集合、未达阈值保持收集中、
       同标识同内容重传回放、改站点/改原因可操作冲突、已隔离站投票不恢复计票、
       封存后隔离请求被拒绝且证书逐字节不变、隔离与临界票并发一致性。
  B. 构建检查：app/verify 全部 Python 源码可编译；页面为含隔离裁决控制台的真实页面。
  C. API/HTTP 冒烟
     - /health 健康响应；
     - 多站并发投票下封签唯一（证书唯一、指纹一致、迟到票拒绝）；
     - 封存后并发冲击不可改写证书。
  D. 重启恢复
     - 复制线上持久记录，拉起全新服务进程（模拟重启），
       验证收集中批次的票与冻结配置恢复、已封存批次仍为 sealed 且证书逐字节一致；
     - 模拟写入途中崩溃残留的临时文件不影响恢复；
     - 二次重启结果不变，迟到票仍被拒绝。

成功退出码 0，任一失败退出码 1。
"""

import json
import os
import shutil
import socket
import subprocess
import sys
import threading
import time
import unittest
import urllib.error
import urllib.request
import uuid

BASE_URL = os.environ.get("BASE_URL", "http://web:8080").rstrip("/")
APP_PATH = os.environ.get("APP_PATH", "/app/server.py")
SOURCE_DATA_DIR = os.environ.get("SOURCE_DATA_DIR", "/data")
RESTART_ROOT = os.environ.get("RESTART_ROOT", "/tmp/restart")

FAILURES = []


def check(cond, msg):
    if cond:
        print(f"    ✅ {msg}")
    else:
        print(f"    ❌ {msg}")
        FAILURES.append(msg)


# ---------------------------------------------------------------- HTTP 客户端

class Http:
    def __init__(self, base):
        self.base = base.rstrip("/")

    def request(self, method, path, body=None, timeout=15):
        url = self.base + path
        data = None
        headers = {"Accept": "application/json"}
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read().decode("utf-8")
                if not raw:
                    return resp.status, None
                try:
                    return resp.status, json.loads(raw)
                except json.JSONDecodeError:
                    return resp.status, raw
        except urllib.error.HTTPError as e:
            raw = e.read().decode("utf-8")
            try:
                return e.code, json.loads(raw)
            except json.JSONDecodeError:
                return e.code, {"raw": raw}

    def get(self, path, **kw):
        return self.request("GET", path, **kw)

    def post(self, path, body, **kw):
        return self.request("POST", path, body=body, **kw)

    def get_batch(self, bid):
        st, body = self.get(f"/api/batches/{bid}")
        return st, body["batch"]

    def create(self, stations, threshold, bid=None):
        bid = bid or f"vt-{uuid.uuid4().hex[:12]}"
        st, body = self.post("/api/batches", {"id": bid, "stations": stations, "threshold": threshold})
        return st, body, bid

    def vote(self, bid, station, digest, vote_id):
        return self.post(f"/api/batches/{bid}/votes",
                         {"station": station, "digest": digest, "vote_id": vote_id})

    def quarantine(self, bid, station, quarantine_id, reason):
        return self.post(f"/api/batches/{bid}/quarantine",
                         {"station": station, "quarantine_id": quarantine_id, "reason": reason})


API = Http(BASE_URL)


def wait_available(max_wait=90):
    print("\n== 等待页面与健康响应可用 ==")
    deadline = time.time() + max_wait
    health_ok = page_ok = False
    while time.time() < deadline:
        try:
            st, body = API.get("/health", timeout=3)
            health_ok = st == 200 and isinstance(body, dict) and body.get("status") == "ok"
        except Exception:
            health_ok = False
        try:
            st, body = API.get("/", timeout=3)
            if st != 200 or not isinstance(body, str):
                page_ok = False
            else:
                page_ok = ("封存控制台" in body and "castVote" in body
                           and "submitQuarantine" in body and "隔离裁决" in body)
        except Exception:
            page_ok = False
        if health_ok and page_ok:
            break
        time.sleep(0.5)
    check(health_ok, f"健康响应可用：GET {BASE_URL}/health → 200 status=ok")
    check(page_ok, "页面可用：GET / 返回 200 且包含控制台界面与投票脚本")
    return health_ok and page_ok


# ---------------------------------------------------------- B. 构建检查

def test_build_checks():
    print("\n== 构建检查：Python 源码编译 + 页面资产 ==")
    import py_compile

    targets = [APP_PATH, os.path.abspath(__file__)]
    ok = True
    for path in targets:
        try:
            py_compile.compile(path, doraise=True)
            check(True, f"编译通过：{path}")
        except py_compile.PyCompileError as e:
            check(False, f"编译失败：{path}：{e}")
            ok = False

    # 页面资产为含隔离裁决表单与渲染逻辑的真实页面（非占位）。
    st, body = API.get("/")
    page_ok = (
        st == 200 and isinstance(body, str)
        and "submitQuarantine" in body and "/quarantine" in body
        and "quarantines" in body and "excluded_yes_stations" in body
        and "QUARANTINE_CONFLICT" in body and "STATION_QUARANTINED" in body
    )
    check(page_ok, "页面包含隔离裁决提交、实况渲染与冲突错误处理")
    return ok and page_ok


# ---------------------------------------------------------- A. 代码测试 unittest

class CodeTests(unittest.TestCase):
    """覆盖幂等重传与冲突票隔离的代码测试。"""

    def test_01_idempotent_replay_replays_first_result_without_new_vote(self):
        st, body, bid = API.create(["站A", "站B", "站C"], 3)
        self.assertEqual(st, 201, body)

        st, r1 = API.vote(bid, "站A", "digest-D1", "sv-A-1")
        self.assertEqual(st, 200, r1)
        self.assertFalse(r1["replayed"])
        self.assertTrue(r1["froze_config"])
        self.assertEqual(r1["batch"]["yes_count"], 1)
        first_vote_id = r1["vote_id"]

        # 同站 + 同摘要 + 同稳定投票标识重传 ×2：只回放，不增票。
        for _ in range(2):
            st, r = API.vote(bid, "站A", "digest-D1", "sv-A-1")
            self.assertEqual(st, 200, r)
            self.assertTrue(r["replayed"], "重传必须被识别为回放")
            self.assertEqual(r["vote_id"], first_vote_id, "回放的必须是首次投票记录")
            self.assertEqual(r["batch"]["yes_count"], 1, "重传不得增票")

        st, b = API.get_batch(bid)
        self.assertEqual(b["yes_count"], 1)
        self.assertEqual(b["digest"], "digest-D1", "首次有效投票冻结摘要")
        self.assertEqual(b["conflicts"], [])

    def test_02_changed_vote_id_is_recorded_conflict_and_isolated(self):
        st, _, bid = API.create(["站A", "站B", "站C"], 3)
        st, r = API.vote(bid, "站A", "digest-D1", "sv-A-1")
        self.assertEqual((st, r["batch"]["yes_count"]), (200, 1))

        # 同站同摘要但更换稳定投票标识 = 绑定内容改变 → 冲突隔离。
        st, r = API.vote(bid, "站A", "digest-D1", "sv-A-TAMPERED")
        self.assertEqual(st, 422, r)
        self.assertEqual(r["error"]["code"], "VOTE_ID_CHANGED")
        st, b = API.get_batch(bid)
        self.assertEqual(b["yes_count"], 1, "冲突票不计入赞成票")
        self.assertEqual(len(b["conflicts"]), 1)
        self.assertEqual(b["conflicts"][0]["station"], "站A")
        self.assertEqual(b["conflicts"][0]["reason"], "VOTE_ID_CHANGED")

        # 同一冲突载荷重传：回放冲突结果，不重复堆积冲突记录。
        st2, r2 = API.vote(bid, "站A", "digest-D1", "sv-A-TAMPERED")
        self.assertEqual(st2, 422)
        self.assertTrue(r2["error"]["details"].get("replayed"), "冲突票重传应为回放")
        st, b = API.get_batch(bid)
        self.assertEqual(len(b["conflicts"]), 1, "冲突重传不新增记录")
        self.assertEqual(b["yes_count"], 1)

        # 原始标识重传仍正常回放，不增票。
        st, r = API.vote(bid, "站A", "digest-D1", "sv-A-1")
        self.assertEqual(st, 200)
        self.assertTrue(r["replayed"])

        # 其余两站赞成到达阈值：冲突站点不参与封存，证书只含有效赞成站。
        st, r = API.vote(bid, "站B", "digest-D1", "sv-B-1")
        self.assertEqual(st, 200, r)
        st, r = API.vote(bid, "站C", "digest-D1", "sv-C-1")
        self.assertEqual(st, 200, r)
        cert = r["batch"]["certificate"]
        self.assertIsNotNone(cert, "达到阈值必须封签")
        self.assertEqual(cert["stations"], ["站A", "站B", "站C"])
        self.assertEqual(len(cert["stations"]), 3)

        # 封存后冲突票/迟到票不得改写证书。
        fp = cert["fingerprint"]
        st, r = API.vote(bid, "站B", "digest-D1", "sv-B-TAMPERED")
        self.assertEqual(st, 409)
        self.assertEqual(r["error"]["code"], "LATE_VOTE_REJECTED")
        st, b = API.get_batch(bid)
        self.assertEqual(b["status"], "sealed")
        self.assertEqual(b["certificate"]["fingerprint"], fp, "证书指纹不可变")

    def test_03_different_digest_is_conflict_and_cannot_seal(self):
        st, _, bid = API.create(["站A", "站B"], 2)
        st, r = API.vote(bid, "站A", "digest-D1", "sv-A-1")
        self.assertEqual(st, 200)

        st, r = API.vote(bid, "站B", "digest-D2", "sv-B-1")
        self.assertEqual(st, 422)
        self.assertEqual(r["error"]["code"], "DIGEST_MISMATCH")
        st, b = API.get_batch(bid)
        self.assertEqual(b["yes_count"], 1, "异摘要票隔离，不增赞成票")
        self.assertEqual(len(b["conflicts"]), 1)
        self.assertIsNone(b["certificate"], "冲突票不得触发封存")

        # 站B 改投冻结摘要 → 达到阈值才封签，证书锚定冻结摘要。
        st, r = API.vote(bid, "站B", "digest-D1", "sv-B-1")
        self.assertEqual(st, 200, r)
        self.assertEqual(r["batch"]["certificate"]["digest"], "digest-D1")

    def test_04_config_mismatch_and_invalid_params_get_actionable_rejections(self):
        st, _, bid = API.create(["站A", "站B"], 2)

        st, r = API.vote(bid, "站X-未授权", "digest-D1", "sv-X-1")
        self.assertEqual(st, 422)
        self.assertEqual(r["error"]["code"], "STATION_NOT_LISTED", r)
        self.assertIn("站A", r["error"]["details"].get("frozen_stations", []),
                      "拒绝反馈须带回冻结名单以便修正")

        st, r, _ = API.create(["站A"], 1, bid=bid)
        self.assertEqual(st, 409)
        self.assertEqual(r["error"]["code"], "BATCH_EXISTS")

        bad_cases = [
            ([{"id": "bad id!", "stations": ["站A"], "threshold": 1}], "INVALID_BATCH_ID"),
            ([{"id": f"x-{uuid.uuid4().hex[:8]}", "stations": [], "threshold": 1}], "INVALID_STATIONS"),
            ([{"id": f"x-{uuid.uuid4().hex[:8]}", "stations": ["站A", "站A"], "threshold": 1}],
             "DUPLICATE_STATION"),
            ([{"id": f"x-{uuid.uuid4().hex[:8]}", "stations": ["站A"], "threshold": 0}], "INVALID_THRESHOLD"),
            ([{"id": f"x-{uuid.uuid4().hex[:8]}", "stations": ["站A"], "threshold": 5}], "INVALID_THRESHOLD"),
            ([{"id": f"x-{uuid.uuid4().hex[:8]}", "stations": ["站A"], "threshold": "2"}],
             "INVALID_THRESHOLD"),
        ]
        for payload, code in bad_cases:
            st, r = API.post("/api/batches", payload[0])
            self.assertEqual(st, 422, payload)
            self.assertEqual(r["error"]["code"], code, payload)

        st, r = API.vote(f"missing-{uuid.uuid4().hex[:6]}", "站A", "d", "v")
        self.assertEqual(st, 404)
        self.assertEqual(r["error"]["code"], "BATCH_NOT_FOUND")

        st, r = API.post(f"/api/batches/{bid}/votes", {"station": "站A", "digest": "d"})
        self.assertEqual(st, 422)
        self.assertEqual(r["error"]["code"], "INVALID_PARAMETER")

    # ---------- 审查员隔离裁决 ----------

    def test_05_quarantine_excludes_prior_yes_and_keeps_collecting(self):
        # 阈值 3，两票赞成（未封存）后隔离站A：既有赞成票排出有效集合，保持收集中；
        # 需 4 座站，使排除站A后 B/C/D 仍可凑齐 3 张有效票。
        st, _, bid = API.create(["站A", "站B", "站C", "站D"], 3)
        st, _ = API.vote(bid, "站A", "digest-Q", "sv-A")
        st, _ = API.vote(bid, "站B", "digest-Q", "sv-B")
        st, r = API.quarantine(bid, "站A", "q-chain-A-1", "地面站测量链异常：复核数据漂移")
        self.assertEqual(st, 200, r)
        self.assertFalse(r["replayed"])
        self.assertTrue(r["excluded_prior_yes"], "站A 既有赞成票应被排出有效集合")
        b = r["batch"]
        self.assertEqual(b["status"], "collecting", "排出后有效票 1 < 阈值 3，必须保持收集中")
        self.assertEqual(b["yes_count"], 1)
        self.assertEqual(b["yes_stations"], ["站B"])
        self.assertEqual(b["excluded_yes_stations"], ["站A"])
        self.assertEqual(len(b["quarantines"]), 1)
        self.assertEqual(b["quarantines"][0]["station"], "站A")
        self.assertEqual(b["quarantines"][0]["reason"], "地面站测量链异常：复核数据漂移")

        # 仅一票补足仍不达阈值（2 < 3），保持收集。
        st, r = API.vote(bid, "站C", "digest-Q", "sv-C")
        self.assertEqual(st, 200)
        self.assertIsNone(r["batch"]["certificate"], "有效票 2/3 不得提前封存")
        self.assertEqual(r["batch"]["yes_count"], 2)

        # 剩余有效赞成到达阈值才封存；证书只含未隔离有效站。
        st, r = API.vote(bid, "站D", "digest-Q", "sv-D")
        self.assertEqual(st, 200)
        cert = r["batch"]["certificate"]
        self.assertIsNotNone(cert)
        self.assertEqual(cert["stations"], ["站B", "站C", "站D"], "证书不得包含被隔离站A")

    def test_06_quarantine_idempotent_replay_and_conflicts(self):
        st, _, bid = API.create(["站A", "站B"], 2)
        st, r = API.quarantine(bid, "站A", "q-idem-1", "原因一")
        self.assertEqual(st, 200, r)
        first_ts = r["ts"]
        rev_after_create = r["batch"]["revision"]

        # 同标识 + 同站点 + 同原因重传：回放首次结果，revision 不增长、时间戳一致。
        st, r = API.quarantine(bid, "站A", "q-idem-1", "原因一")
        self.assertEqual(st, 200, r)
        self.assertTrue(r["replayed"])
        self.assertEqual(r["ts"], first_ts)
        self.assertEqual(r["batch"]["revision"], rev_after_create, "回放不得重复变更")
        st, b = API.get_batch(bid)
        self.assertEqual(len(b["quarantines"]), 1)

        # 同标识改用不同原因：可操作冲突，且不改变既有裁决。
        st, r = API.quarantine(bid, "站A", "q-idem-1", "原因二-篡改")
        self.assertEqual(st, 409, r)
        self.assertEqual(r["error"]["code"], "QUARANTINE_CONFLICT")
        d = r["error"]["details"]
        self.assertEqual(d["existing"]["reason"], "原因一")
        self.assertEqual(d["submitted"]["reason"], "原因二-篡改")
        self.assertIn("hint", d)

        # 同标识改用不同站点：可操作冲突。
        st, r = API.quarantine(bid, "站B", "q-idem-1", "原因一")
        self.assertEqual(st, 409)
        self.assertEqual(r["error"]["code"], "QUARANTINE_CONFLICT")
        self.assertEqual(r["error"]["details"]["existing"]["station"], "站A")
        self.assertEqual(r["error"]["details"]["submitted"]["station"], "站B")

        # 对已隔离站用新标识：同样给出冲突反馈，记录不增加。
        st, r = API.quarantine(bid, "站A", "q-idem-2", "原因一")
        self.assertEqual(st, 409)
        self.assertEqual(r["error"]["code"], "QUARANTINE_CONFLICT")
        st, b = API.get_batch(bid)
        self.assertEqual(len(b["quarantines"]), 1, "冲突提交不得新增隔离记录")
        self.assertEqual(b["revision"], rev_after_create)

    def test_07_quarantined_station_votes_never_rejoin_count(self):
        st, _, bid = API.create(["站A", "站B", "站C"], 2)
        st, _ = API.vote(bid, "站A", "digest-Q", "sv-A")
        st, _ = API.quarantine(bid, "站A", "q-vote-1", "链路异常")
        st, b = API.get_batch(bid)
        self.assertEqual(b["yes_count"], 0)

        # 原赞成票重传：幂等回放，但仍不计票。
        st, r = API.vote(bid, "站A", "digest-Q", "sv-A")
        self.assertEqual(st, 200)
        self.assertTrue(r["replayed"])
        self.assertEqual(r["batch"]["yes_count"], 0)

        # 新载荷（新 vote_id）：明确拒绝，不得恢复计票。
        st, r = API.vote(bid, "站A", "digest-Q", "sv-A-NEW")
        self.assertEqual(st, 422, r)
        self.assertEqual(r["error"]["code"], "STATION_QUARANTINED")

        # 不同摘要的新载荷同样被隔离闸口拒绝（而非计票或冲突票）。
        st, r = API.vote(bid, "站A", "digest-OTHER", "sv-A-OTHER")
        self.assertEqual(st, 422)
        self.assertEqual(r["error"]["code"], "STATION_QUARANTINED")

        st, b = API.get_batch(bid)
        self.assertEqual(b["yes_count"], 0)
        self.assertEqual(b["yes_stations"], [])
        self.assertIsNone(b["certificate"], "被隔离站无法推动封存")

    def test_08_quarantine_after_seal_rejected_cert_byte_identical(self):
        st, _, bid = API.create(["站A", "站B"], 2)
        st, _ = API.vote(bid, "站A", "digest-S", "sv-A")
        st, r = API.vote(bid, "站B", "digest-S", "sv-B")
        cert_before = r["batch"]["certificate"]
        self.assertIsNotNone(cert_before)
        canon = lambda c: json.dumps(c, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        cert_bytes_before = canon(cert_before)

        # 封存后的隔离请求必须被拒绝。
        st, r = API.quarantine(bid, "站A", "q-late-1", "封存后才发现异常")
        self.assertEqual(st, 409, r)
        self.assertEqual(r["error"]["code"], "BATCH_SEALED")
        self.assertEqual(r["error"]["details"]["certificate_fingerprint"],
                         cert_before["fingerprint"])

        # 证书逐字节不变。
        st, b = API.get_batch(bid)
        self.assertEqual(b["status"], "sealed")
        self.assertEqual(canon(b["certificate"]), cert_bytes_before, "证书逐字节不变")
        self.assertEqual(b["quarantines"], [], "封存后不得写入任何隔离记录")

    def test_09_quarantine_station_not_listed_and_invalid_params(self):
        st, _, bid = API.create(["站A"], 1)
        for payload, code in [
            ({"station": "站X", "quarantine_id": "q-x", "reason": "r"}, "STATION_NOT_LISTED"),
            ({"station": "站A", "quarantine_id": "", "reason": "r"}, "INVALID_PARAMETER"),
            ({"station": "站A", "quarantine_id": "q", "reason": "   "}, "INVALID_PARAMETER"),
            ({"station": 42, "quarantine_id": "q", "reason": "r"}, "INVALID_PARAMETER"),
        ]:
            st, r = API.post(f"/api/batches/{bid}/quarantine", payload)
            self.assertEqual(st, 422, (payload, r))
            self.assertEqual(r["error"]["code"], code, payload)

        st, r = API.quarantine(f"missing-{uuid.uuid4().hex[:6]}", "站A", "q", "r")
        self.assertEqual(st, 404)
        self.assertEqual(r["error"]["code"], "BATCH_NOT_FOUND")

    def test_10_quarantine_persisted_to_disk(self):
        # 隔离裁决必须已原子落盘：直接读取线上持久记录文件校验。
        st, _, bid = API.create(["pA", "pB", "pC"], 2)
        st, _ = API.vote(bid, "pA", f"D-{bid}", "vid-pA")
        st, r = API.quarantine(bid, "pA", f"q-{bid}", "测量链异常持久化")
        self.assertEqual(st, 200, r)

        db_path = os.path.join(SOURCE_DATA_DIR, "seal.db.json")
        with open(db_path, "r", encoding="utf-8") as f:
            db = json.load(f)
        rec = db["batches"][bid]
        self.assertEqual(len(rec["quarantines"]), 1)
        self.assertEqual(rec["quarantines"][0]["quarantine_id"], f"q-{bid}")
        self.assertEqual(rec["quarantines"][0]["station"], "pA")
        self.assertIsNone(rec["certificate"], "有效票 0/2，落盘记录必须仍为未封存")


# ------------------------------------------------------------- C. 并发封签唯一性

def test_concurrent_seal_uniqueness():
    print("\n== 并发封签唯一性 ==")
    stations = [f"c{i}" for i in range(5)]
    st, _, bid = API.create(stations, 3)
    check(st == 201, f"批次 {bid} 创建：5 站、阈值 3")
    digest = f"D-{bid}"

    results = []
    barrier = threading.Barrier(len(stations) * 4)

    def worker(station, seq):
        barrier.wait()
        results.append((station, seq, API.vote(bid, station, digest, f"vid-{station}")))

    threads = []
    for s in stations:
        for seq in range(4):  # 每站 1 票 + 3 次完全相同的并发重传
            t = threading.Thread(target=worker, args=(s, seq))
            threads.append(t)
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    accepted = [r for r in results if r[2][0] == 200 and not r[2][1].get("replayed")]
    replayed = [r for r in results if r[2][0] == 200 and r[2][1].get("replayed")]
    late = [r for r in results if r[2][0] == 409]
    check(len(accepted) == 3, f"20 个并发请求中恰好 3 票首次生效（实际 {len(accepted)}）")
    check(len(replayed) >= 1, f"存在并发幂等回放（{len(replayed)} 个）")
    check(len(late) >= 1, f"阈值后到达的票被迟到拒绝（{len(late)} 个 409）")

    fps = set()
    for _, _, (st_, body) in results:
        cert = (body.get("batch") or {}).get("certificate") if isinstance(body, dict) else None
        if cert:
            fps.add(cert["fingerprint"])
    check(len(fps) == 1, f"所有封签响应中的证书指纹唯一（{fps or '无证书！'}）")

    st, b = API.get_batch(bid)
    check(b["status"] == "sealed", "批次最终状态为 sealed")
    check(b["yes_count"] == 3, f"赞成站数锁定在阈值 3（实际 {b['yes_count']}）")
    check(len(b["certificate"]["stations"]) == 3, "证书恰好列出 3 个赞成站")
    fp_final = b["certificate"]["fingerprint"]

    # 并发冲突票与篡改票在封存后冲击：证书绝不改写。
    hammer = []

    def hammer_worker(i):
        if i % 3 == 0:
            hammer.append(API.vote(bid, stations[i % 5], digest, f"vid-changed-{i}"))
        elif i % 3 == 1:
            hammer.append(API.vote(bid, stations[i % 5], f"digest-EVIL-{i}", f"vid-{stations[i % 5]}"))
        else:
            hammer.append(API.vote(bid, "站X-未授权", digest, "vid-x"))

    ths = [threading.Thread(target=hammer_worker, args=(i,)) for i in range(30)]
    for t in ths: t.start()
    for t in ths: t.join()
    check(all(s in (409, 422) for s, _ in hammer), "封存后全部冲击票被 409/422 拒绝")
    st, b = API.get_batch(bid)
    check(b["certificate"]["fingerprint"] == fp_final, "30 个并发冲击后证书指纹不变")

    # 并发异摘要票在封签前隔离：预置一票冻结正确摘要，再并发投票。
    st2, _, bid2 = API.create([f"x{i}" for i in range(6)], 5)
    digest2 = f"D-{bid2}"
    st, _ = API.vote(bid2, "x0", digest2, "vid-x0")
    check(st == 200, "预置 x0 赞成票冻结正确摘要")
    res2 = []
    jobs = [(f"x{i}", False) for i in range(1, 5)] + [("x5", True)]
    barrier2 = threading.Barrier(len(jobs))

    def worker2(station, bad_digest=False):
        barrier2.wait()
        d = "digest-WRONG" if bad_digest else digest2
        res2.append(API.vote(bid2, station, d, f"vid-{station}"))

    ths = [threading.Thread(target=worker2, args=(*j,)) for j in jobs]
    for t in ths: t.start()
    for t in ths: t.join()
    st, b2 = API.get_batch(bid2)
    check(b2["status"] == "sealed", "5 个有效赞成站并发到达后封签")
    check(b2["certificate"]["stations"] == [f"x{i}" for i in range(5)],
          "证书只含 5 个有效站，异摘要站 x5 被排除")
    check(b2["yes_count"] == 5, "冲突票不计入赞成票")


# ------------------------------------------------------------- C2. 隔离/临界票/封签并发

def test_concurrent_quarantine_interleave():
    print("\n== 隔离裁决与临界赞成票/封签并发 ==")
    # 5 站、阈值 3：预置 q0 一票；临界区让 q1/q2 两票与“隔离 q1”同时到达。
    # 两种严格互斥的结局：
    #   封签先提交 → q0/q1/q2 达阈值封存，隔离请求 409，无隔离记录；
    #   隔离先提交 → q1 的票被隔离闸口拒绝（422），仅 q2 生效，有效票 2 < 3，保持收集中。
    stations = [f"q{i}" for i in range(5)]
    st, _, bid = API.create(stations, 3)
    digest = f"D-{bid}"
    st, _ = API.vote(bid, "q0", digest, "vid-q0")
    check(st == 200, f"批次 {bid} 预置 q0 一票（有效 1/3）")

    outcomes = []
    jobs = [("vote", "q1"), ("vote", "q2"), ("quarantine", "q1")]
    barrier = threading.Barrier(len(jobs))

    def worker(kind, station):
        barrier.wait()
        if kind == "vote":
            outcomes.append((kind, station, API.vote(bid, station, digest, f"vid-{station}")))
        else:
            outcomes.append((kind, station,
                             API.quarantine(bid, station, f"qid-{bid}-q1", "并发到达的隔离裁决")))

    ths = [threading.Thread(target=worker, args=j) for j in jobs]
    for t in ths: t.start()
    for t in ths: t.join()

    vote_q1 = [o for o in outcomes if o[0] == "vote" and o[1] == "q1"][0][2]
    q_res = [o for o in outcomes if o[0] == "quarantine"][0][2]

    st, b = API.get_batch(bid)
    if b["status"] == "sealed":
        # 情形一：封签先于隔离临界区提交。
        check(q_res[0] == 409 and q_res[1]["error"]["code"] == "BATCH_SEALED",
              "封存先行：并发隔离请求被 BATCH_SEALED 拒绝")
        check(vote_q1[0] == 200 and not vote_q1[1].get("replayed"),
              "封存先行：q1 临界票在封存前生效")
        check(b["certificate"]["stations"] == ["q0", "q1", "q2"],
              "封存先行：证书恰好为达到阈值的 3 个有效站")
        check(b["quarantines"] == [], "封存先行：未写入任何隔离记录")
        print("    ℹ️ 本次交错结果：封签先提交，证书 =", b["certificate"]["stations"])
    else:
        # 情形二：隔离在封签临界票（达到阈值的第 3 票）之前提交 → 必保持收集中。
        check(b["status"] == "collecting", "隔离先行：批次保持收集中（有效票不足阈值）")
        check(q_res[0] == 200 and q_res[1]["accepted"] and not q_res[1]["replayed"],
              "隔离先行：裁决正常生效")
        # q1 的票与隔离的先后有两种合法串行化，结果都必须一致：
        if vote_q1[0] == 200:
            check(q_res[1]["excluded_prior_yes"] is True,
                  "隔离先行(票先到)：裁决须报告排出了 q1 既有赞成票")
            check("q1" in b["excluded_yes_stations"], "隔离先行(票先到)：q1 标记为已排出")
        else:
            check(vote_q1[0] == 422 and vote_q1[1]["error"]["code"] == "STATION_QUARANTINED",
                  "隔离先行(裁决先到)：后到的 q1 投票不得恢复计票（422 STATION_QUARANTINED）")
        check(b["yes_count"] == 2 and b["yes_stations"] == ["q0", "q2"],
              f"隔离先行：有效赞成仅 q0/q2（实际 {b['yes_stations']}）")
        check([q["station"] for q in b["quarantines"]] == ["q1"], "隔离先行：仅 q1 被隔离")
        check(b["certificate"] is None, "隔离先行：无证书")
        print("    ℹ️ 本次交错结果：隔离先提交，有效赞成 =", b["yes_stations"])

    # 不变量：任何最终证书都不得包含被隔离站。
    if b["certificate"]:
        qset = {q["station"] for q in b["quarantines"]}
        check(not (set(b["certificate"]["stations"]) & qset),
              "最终快照：证书与隔离集合不矛盾")

    # 并发同标识重传：先确立首次裁决，再并发 3 个完全相同（回放）+ 3 个改原因（冲突）。
    st, _, bid2 = API.create([f"m{i}" for i in range(5)], 4)
    qid = f"qid-{bid2}-same"
    st, r0 = API.quarantine(bid2, "m0", qid, "测量链异常")
    check(st == 200 and not r0["replayed"], "并发冲击前置：首次隔离裁决已生效")
    results = []
    barrier2 = threading.Barrier(6)

    def q_worker(i):
        barrier2.wait()
        reason = "测量链异常" if i < 3 else f"不同原因-{i}"
        results.append(API.quarantine(bid2, "m0", qid, reason))

    ths = [threading.Thread(target=q_worker, args=(i,)) for i in range(6)]
    for t in ths: t.start()
    for t in ths: t.join()
    created = [r for r in results if r[0] == 200 and not r[1].get("replayed")]
    replayed = [r for r in results if r[0] == 200 and r[1].get("replayed")]
    conflicts = [r for r in results if r[0] == 409]
    check(len(created) == 0, f"并发重传/冲突均不产生新生效裁决（实际 {len(created)}）")
    check(len(replayed) == 3, f"3 个并发相同内容被回放（{len(replayed)} 个）")
    check(len(conflicts) == 3, f"3 个并发不同原因得到可操作冲突（{len(conflicts)} 个）")
    st, b2 = API.get_batch(bid2)
    check(len(b2["quarantines"]) == 1, "并发冲击后隔离记录仍唯一")
    check(b2["quarantines"][0]["reason"] == "测量链异常", "首次原因不被并发改写")


# ---------------------------------------------------------------- D. 重启恢复

def _free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _start_server(data_dir, port):
    env = dict(os.environ)
    env.update({
        "DATA_DIR": data_dir,
        "SEAL_PORT": str(port),
        "SEAL_HOST": "127.0.0.1",
        "PYTHONUNBUFFERED": "1",
    })
    proc = subprocess.Popen(
        [sys.executable, APP_PATH],
        env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    api = Http(f"http://127.0.0.1:{port}")
    deadline = time.time() + 30
    while time.time() < deadline:
        if proc.poll() is not None:
            out = proc.stdout.read() if proc.stdout else ""
            raise RuntimeError(f"恢复服务进程提前退出\n{out}")
        try:
            st, body = api.get("/health", timeout=2)
            if st == 200 and body.get("status") == "ok":
                return proc, api
        except Exception:
            time.sleep(0.3)
    proc.kill()
    raise RuntimeError("恢复服务 30s 内未就绪")


def test_restart_recovery():
    print("\n== 重启恢复（以持久记录冷启动新进程）==")
    # 线上服务准备：一个收集中批次（1 票），一个已封存批次（阈值 1）。
    _, _, collecting = API.create(["rA", "rB"], 2)
    st, r = API.vote(collecting, "rA", f"D-{collecting}", "vid-rA")
    check(st == 200 and r["batch"]["yes_count"] == 1, "收集中批次已含 1 张赞成票")

    _, _, sealed = API.create(["sA"], 1)
    st, r = API.vote(sealed, "sA", f"D-{sealed}", "vid-sA")
    check(st == 200 and r["batch"]["certificate"] is not None, "已封存批次持有证书")
    st, live_sealed = API.get_batch(sealed)
    live_cert = live_sealed["certificate"]

    # 收集中批次 + 一张冲突票：验证冲突记录同样持久化、重启后继续隔离。
    _, _, conflicting = API.create(["cA", "cB"], 2)
    st, _ = API.vote(conflicting, "cA", f"D-{conflicting}", "vid-cA")
    st, r = API.vote(conflicting, "cB", "digest-WRONG", "vid-cB-wrong")
    check(st == 422 and r["error"]["code"] == "DIGEST_MISMATCH", "收集中批次含 1 张冲突票")

    # 收集中批次 + 审查员隔离裁决（被隔离站曾投赞成票，已排出有效集合）。
    _, _, quarantined = API.create(["qA", "qB", "qC"], 2)
    st, _ = API.vote(quarantined, "qA", f"D-{quarantined}", "vid-qA")
    st, r = API.quarantine(quarantined, "qA", f"qid-{quarantined}", "测量链异常-持久化")
    check(st == 200 and r["batch"]["yes_count"] == 0
          and r["batch"]["excluded_yes_stations"] == ["qA"],
          "收集中批次含 1 条隔离裁决，qA 赞成票已排出有效集合")

    # 已封存批次封存后再请求隔离：必须拒绝，记录拒绝前的证书用于逐字节比对。
    st, r = API.quarantine(sealed, "sA", f"qid-late-{sealed}", "封存后异常")
    check(st == 409 and r["error"]["code"] == "BATCH_SEALED", "封存后隔离请求被拒绝")

    src_db = os.path.join(SOURCE_DATA_DIR, "seal.db.json")
    for attempt in range(30):
        if os.path.exists(src_db):
            break
        time.sleep(0.5)
    check(os.path.exists(src_db), f"读到线上持久记录 {src_db}")

    data_dir = os.path.join(RESTART_ROOT, "boot1")
    shutil.rmtree(data_dir, ignore_errors=True)
    os.makedirs(data_dir, exist_ok=True)
    shutil.copy2(src_db, os.path.join(data_dir, "seal.db.json"))
    # 模拟“写入投票途中异常中断”：正式文件之外残留半截临时文件。
    with open(os.path.join(data_dir, "seal.db.json.tmp.crash-9999"), "w") as f:
        f.write('{"version":1, "batches": {"半截内容')

    port = _free_port()
    proc, rapi = _start_server(data_dir, port)
    try:
        leftovers = [n for n in os.listdir(data_dir) if n.startswith("seal.db.json.tmp.")]
        check(not leftovers, "冷启动清理崩溃残留的临时文件，正式记录完好")

        st, b = rapi.get_batch(collecting)
        check(st == 200 and b["status"] == "collecting", "收集中批次恢复为 collecting")
        check(b["yes_count"] == 1 and b["digest"] == f"D-{collecting}",
              "冻结摘要与赞成票从持久记录恢复")

        # 冲突票持久化恢复：重传回放首次冲突，正确票仍可让批次封签。
        st, b = rapi.get_batch(conflicting)
        check(b["yes_count"] == 1 and len(b["conflicts"]) == 1,
              "冲突记录从持久记录恢复（1 赞成 + 1 冲突）")
        st, r = rapi.vote(conflicting, "cB", "digest-WRONG", "vid-cB-wrong")
        check(st == 422 and r["error"]["details"].get("replayed") is True,
              "恢复后冲突票重传仍为回放，不重复堆积")
        st, r = rapi.vote(conflicting, "cB", f"D-{conflicting}", "vid-cB")
        check(st == 200 and r["batch"]["status"] == "sealed"
              and r["batch"]["conflicts"], "冲突不影响正确票到达阈值封签，证书与冲突记录并存")
        conf_fp = r["batch"]["certificate"]["fingerprint"]

        # 隔离裁决恢复：记录、原因、有效票数与未封存状态全部还原。
        st, b = rapi.get_batch(quarantined)
        check(st == 200 and b["status"] == "collecting", "隔离批次恢复为 collecting（未封存状态保持）")
        check(b["yes_count"] == 0 and b["excluded_yes_stations"] == ["qA"],
              "恢复后有效票数仍为 0，qA 既有赞成票保持排出")
        check(len(b["quarantines"]) == 1
              and b["quarantines"][0]["quarantine_id"] == f"qid-{quarantined}"
              and b["quarantines"][0]["reason"] == "测量链异常-持久化",
              "隔离记录（标识 + 站点 + 原因）逐字恢复")

        # 恢复后：同标识同内容重传回放；被隔离站投票不恢复计票。
        st, r = rapi.quarantine(quarantined, "qA", f"qid-{quarantined}", "测量链异常-持久化")
        check(st == 200 and r.get("replayed") is True, "恢复后隔离裁决同内容重传为回放")
        st, r = rapi.vote(quarantined, "qA", f"D-{quarantined}", "vid-qA-NEW")
        check(st == 422 and r["error"]["code"] == "STATION_QUARANTINED",
              "恢复后被隔离站新载荷投票仍不得恢复计票")

        # 未隔离站补足有效票 → 仅按未隔离有效票唯一封存，证书不含 qA。
        st, r = rapi.vote(quarantined, "qB", f"D-{quarantined}", "vid-qB")
        check(st == 200 and r["batch"]["status"] == "collecting", "qB 一票后有效 1/2 仍收集中")
        st, r = rapi.vote(quarantined, "qC", f"D-{quarantined}", "vid-qC")
        check(st == 200 and r["batch"]["status"] == "sealed", "qC 到达阈值后封存")
        check(r["batch"]["certificate"]["stations"] == ["qB", "qC"],
              "恢复后封存证书只含未隔离有效站 qB/qC")
        quar_fp = r["batch"]["certificate"]["fingerprint"]

        st, b = rapi.get_batch(sealed)
        check(st == 200 and b["status"] == "sealed", "已封存批次恢复后仍为 sealed，不回收集中")
        check(b["certificate"] == live_cert, "恢复出的证书与线上逐字节一致（含指纹与封存时间）")

        # 迟到票在恢复后的实例上同样被拒绝。
        st, r = rapi.vote(sealed, "sA", f"D-{sealed}", "vid-late")
        check(st == 409 and r["error"]["code"] == "LATE_VOTE_REJECTED",
              "恢复后迟到票仍被拒绝，不改写证书")

        # 收集中批次可继续推进并在恢复实例上封签。
        st, r = rapi.vote(collecting, "rB", f"D-{collecting}", "vid-rB")
        check(st == 200 and r["batch"]["certificate"] is not None, "恢复后收集中批次可继续并封签")
        rc_fp = r["batch"]["certificate"]["fingerprint"]
    finally:
        proc.kill()
        proc.wait(timeout=10)

    # 再次冷启动（第二次重启），结果不变。
    with open(os.path.join(data_dir, "seal.db.json.tmp.crash-again"), "w") as f:
        f.write("garbage")
    port = _free_port()
    proc2, rapi2 = _start_server(data_dir, port)
    try:
        leftovers = [n for n in os.listdir(data_dir) if n.startswith("seal.db.json.tmp.")]
        check(not leftovers, "第二次冷启动同样清理残留临时文件")
        st, b = rapi2.get_batch(collecting)
        check(b["status"] == "sealed" and b["certificate"]["fingerprint"] == rc_fp,
              "第二次重启：新封存批次仍 sealed 且指纹不变")
        st, b = rapi2.get_batch(sealed)
        check(b["certificate"] == live_cert, "第二次重启：历史证书依然逐字节一致")
        st, b = rapi2.get_batch(quarantined)
        check(b["status"] == "sealed" and b["certificate"]["stations"] == ["qB", "qC"]
              and b["certificate"]["fingerprint"] == quar_fp,
              "第二次重启：隔离批次封存证书不变（只含未隔离有效站）")
        check(b["excluded_yes_stations"] == ["qA"]
              and b["quarantines"][0]["quarantine_id"] == f"qid-{quarantined}",
              "第二次重启：隔离记录与排出状态保持")
        st, r = rapi2.quarantine(quarantined, "qB", f"qid-post-{quarantined}", "封存后隔离")
        check(st == 409 and r["error"]["code"] == "BATCH_SEALED",
              "第二次重启后：封存批次的隔离请求仍被拒绝")
        st, r = rapi2.vote(collecting, "rA", f"D-{collecting}", "vid-rA")
        check(st == 200 and r.get("replayed") is True,
              "已统计过的投票重启后重传仍为幂等回放")
    finally:
        proc2.kill()
        proc2.wait(timeout=10)


# ---------------------------------------------------------------------- 主流程

def run_unittest_suite():
    print("\n== 代码测试：幂等重传 / 冲突隔离 / 隔离裁决 / 参数校验 ==")
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromTestCase(CodeTests)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    if not result.wasSuccessful():
        FAILURES.append(f"unittest 失败 {len(result.failures) + len(result.errors)} 项")
    return result.wasSuccessful()


def main():
    t0 = time.time()
    print(f"目标服务：{BASE_URL}")
    if not wait_available():
        print("页面或健康响应不可用，终止验收。")
        return 1

    # 列表接口冒烟。
    st, body = API.get("/api/batches")
    check(st == 200 and isinstance(body.get("batches"), list), "GET /api/batches 返回批次列表")

    run_unittest_suite()
    test_build_checks()
    try:
        test_concurrent_seal_uniqueness()
    except Exception as e:
        FAILURES.append(f"并发封签测试异常: {e!r}")
        print(f"    ❌ 并发测试异常: {e!r}")
    try:
        test_concurrent_quarantine_interleave()
    except Exception as e:
        FAILURES.append(f"并发隔离交错测试异常: {e!r}")
        print(f"    ❌ 并发隔离交错测试异常: {e!r}")
    try:
        test_restart_recovery()
    except Exception as e:
        FAILURES.append(f"重启恢复测试异常: {e!r}")
        import traceback
        traceback.print_exc()

    print("\n" + "=" * 64)
    if FAILURES:
        print(f"验收失败：{len(FAILURES)} 项未通过，用时 {time.time() - t0:.1f}s")
        for f in FAILURES:
            print(" -", f)
        return 1
    print(f"✅ 验收全部通过：页面、健康、幂等、冲突隔离、审查员隔离裁决、"
          f"并发唯一封签/交错一致性、封存拒绝且证书不变、重启恢复，"
          f"用时 {time.time() - t0:.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
