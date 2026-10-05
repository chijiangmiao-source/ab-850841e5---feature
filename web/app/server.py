#!/usr/bin/env python3
"""深空辐照标定封存服务（标准库实现，无第三方依赖）。

核心规则：
  * 首次有效投票原子冻结：站点名单、阈值、摘要摘要(digest)。
  * 同站 + 同摘要 + 同稳定投票标识的重传只回放首次结果，不增票。
  * 绑定内容改变（不同投票标识）或不同摘要 -> 记录冲突票，隔离且不计入赞成票。
  * 审查员可在批次封存前提交隔离裁决（稳定隔离标识 + 原因）：被隔离站及其既有
    赞成票立即排出有效集合；相同标识及相同内容重传只回放首次裁决，不重复变更；
    同一标识改用不同站点或原因返回可操作冲突。已隔离站的后续投票（含重传）永不计票。
  * 赞成站数达到阈值的瞬间在同一把锁、同一次落盘内生成唯一不可变证书；
    隔离、临界票与封签在全局锁下串行，最终快照要么按未隔离有效票唯一封存，
    要么保持收集中并记录隔离；封存后的隔离请求一律拒绝，证书逐字节不变。
  * 每次变更以 临时文件 + fsync + rename 原子落盘；重启后从持久记录恢复，
    隔离记录、有效票数与封存状态一并恢复，已封存批次恢复后仍为 sealed。
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sys
import threading
import traceback
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import unquote, urlparse

DATA_DIR = os.environ.get("DATA_DIR", "/data")
DB_PATH = os.environ.get("DB_PATH") or os.path.join(DATA_DIR, "seal.db.json")
BIND_HOST = os.environ.get("SEAL_HOST", "0.0.0.0")
BIND_PORT = int(os.environ.get("SEAL_PORT", "8080"))

MAX_BODY_BYTES = 64 * 1024
MAX_STATIONS = 100
STATION_MAX_LEN = 64
DIGEST_MAX_LEN = 200
VOTE_ID_MAX_LEN = 200
QUARANTINE_ID_MAX_LEN = 100
QUARANTINE_REASON_MAX_LEN = 500
BATCH_ID_RE = re.compile(r"^[A-Za-z0-9_.@:/\-]{1,64}$")

VOTE_KIND_YES = "yes"
VOTE_KIND_CONFLICT = "conflict"


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


class ApiError(Exception):
    """带机器可读 code 与 HTTP 状态码的业务异常。"""

    def __init__(self, status: int, code: str, message: str, details=None):
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message
        self.details = details or {}

    def body(self) -> dict:
        return {"error": {"code": self.code, "message": self.message, "details": self.details}}


def _require_str(value, field: str, max_len: int):
    if not isinstance(value, str):
        raise ApiError(422, "INVALID_PARAMETER", f"参数 {field} 必须是字符串", {"field": field})
    value = value.strip()
    if not value:
        raise ApiError(422, "INVALID_PARAMETER", f"参数 {field} 不能为空", {"field": field})
    if len(value) > max_len:
        raise ApiError(422, "INVALID_PARAMETER", f"参数 {field} 长度超过 {max_len}", {"field": field, "max": max_len})
    return value


class Store:
    """线程安全、崩溃安全的批次存储。"""

    def __init__(self, path: str):
        self.path = path
        self.lock = threading.RLock()
        self.state = {"version": 1, "revision": 0, "batches": {}}
        self._load()

    # ---------- 持久化 ----------

    def _load(self):
        d = os.path.dirname(self.path) or "."
        os.makedirs(d, exist_ok=True)
        # 清理上次崩溃可能残留的临时文件（正式文件从不以 .tmp 结尾）。
        for name in os.listdir(d):
            if name.startswith("seal.db.json.tmp."):
                try:
                    os.unlink(os.path.join(d, name))
                except OSError:
                    pass
        if os.path.exists(self.path):
            with open(self.path, "r", encoding="utf-8") as f:
                loaded = json.load(f)
            if not isinstance(loaded, dict) or "batches" not in loaded:
                raise RuntimeError(f"持久记录格式损坏: {self.path}")
            self.state = loaded
            sealed = sum(1 for b in self.state["batches"].values() if b.get("certificate"))
            print(
                f"[recover] 从 {self.path} 恢复：批次 {len(self.state['batches'])} 个，"
                f"已封存 {sealed} 个，revision={self.state.get('revision')}",
                file=sys.stderr,
            )
        else:
            self._persist_locked()

    def _persist_locked(self):
        """调用方必须持有 self.lock。临时文件 + fsync + 原子 rename。"""
        d = os.path.dirname(self.path) or "."
        os.makedirs(d, exist_ok=True)
        tmp = f"{self.path}.tmp.{os.getpid()}.{threading.get_ident()}"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self.state, f, ensure_ascii=False, indent=2, sort_keys=True)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.path)
        dfd = os.open(d, os.O_RDONLY)
        try:
            os.fsync(dfd)
        finally:
            os.close(dfd)

    # ---------- 快照 ----------

    @staticmethod
    def _quarantined_map(b) -> dict:
        return {q["station"]: q for q in b.get("quarantines", [])}

    @staticmethod
    def _effective_yes_votes(b):
        """未隔离站点的赞成票；被裁决隔离的站点既有赞成票全部排出有效集合。"""
        quarantined = {q["station"] for q in b.get("quarantines", [])}
        return [v for v in b["votes"] if v["kind"] == VOTE_KIND_YES and v["station"] not in quarantined]

    @staticmethod
    def _snapshot(b: dict, revision: int) -> dict:
        quarantined = {q["station"] for q in b.get("quarantines", [])}
        yes_votes = [v for v in b["votes"] if v["kind"] == VOTE_KIND_YES and v["station"] not in quarantined]
        yes_stations = sorted({v["station"] for v in yes_votes})
        conflicts = [
            {
                "station": v["station"],
                "digest": v["digest"],
                "vote_id": v["vote_id"],
                "reason": v["reason"],
                "ts": v["ts"],
            }
            for v in b["votes"]
            if v["kind"] == VOTE_KIND_CONFLICT
        ]
        yes_station_set = set(yes_stations)
        def _q_view(q):
            # 被隔离站必不在有效集合；had_yes 表示其是否存在被排出的既有赞成票。
            had_yes = any(
                v["station"] == q["station"] and v["kind"] == VOTE_KIND_YES for v in b["votes"]
            )
            return {
                "id": q["id"],
                "station": q["station"],
                "reason": q["reason"],
                "ts": q["ts"],
                "had_yes_vote": had_yes,
                "yes_vote_excluded": had_yes and q["station"] not in yes_station_set,
            }

        quarantines = [_q_view(q) for q in b.get("quarantines", [])]
        return {
            "id": b["id"],
            "status": "sealed" if b["certificate"] else "collecting",
            "stations": list(b["stations"]),
            "threshold": b["threshold"],
            "digest": b["digest"],
            "frozen_at": b["frozen_at"],
            "yes_count": len(yes_stations),
            "yes_stations": yes_stations,
            "conflicts": conflicts,
            "quarantines": quarantines,
            "quarantined_stations": sorted(quarantined),
            "certificate": b["certificate"],
            "revision": revision,
        }

    def get_batch_snapshot(self, batch_id: str) -> dict:
        with self.lock:
            b = self.state["batches"].get(batch_id)
            if b is None:
                raise ApiError(404, "BATCH_NOT_FOUND", f"批次 {batch_id} 不存在", {"batch_id": batch_id})
            return self._snapshot(b, self.state["revision"])

    def list_snapshots(self) -> list:
        with self.lock:
            return [self._snapshot(b, self.state["revision"]) for b in
                    sorted(self.state["batches"].values(), key=lambda x: x["id"])]

    # ---------- 批次创建（配置录入） ----------

    def create_batch(self, batch_id, stations, threshold):
        batch_id = _require_str(batch_id, "id", 64)
        if not BATCH_ID_RE.match(batch_id):
            raise ApiError(
                422, "INVALID_BATCH_ID",
                "批次标识仅限字母数字与 _ . @ : / -，长度 1-64",
                {"batch_id": batch_id},
            )
        if not isinstance(stations, list) or not stations:
            raise ApiError(422, "INVALID_STATIONS", "站点名单必须是非空数组", {})
        if len(stations) > MAX_STATIONS:
            raise ApiError(422, "INVALID_STATIONS", f"站点数量超过上限 {MAX_STATIONS}", {"max": MAX_STATIONS})
        cleaned = []
        seen = set()
        for raw in stations:
            s = _require_str(raw, "stations", STATION_MAX_LEN)
            if s in seen:
                raise ApiError(422, "DUPLICATE_STATION", f"站点名单存在重复站点: {s}", {"station": s})
            seen.add(s)
            cleaned.append(s)
        if isinstance(threshold, bool) or not isinstance(threshold, int):
            raise ApiError(422, "INVALID_THRESHOLD", "阈值必须是整数", {})
        if not 1 <= threshold <= len(cleaned):
            raise ApiError(
                422, "INVALID_THRESHOLD",
                f"阈值必须在 1 与站点数({len(cleaned)})之间",
                {"threshold": threshold, "station_count": len(cleaned)},
            )
        with self.lock:
            if batch_id in self.state["batches"]:
                existing = self.state["batches"][batch_id]
                if existing["certificate"]:
                    raise ApiError(409, "BATCH_SEALED", f"批次 {batch_id} 已封存，不可重建",
                                   {"batch_id": batch_id})
                raise ApiError(409, "BATCH_EXISTS", f"批次 {batch_id} 已存在且在收集中",
                               {"batch_id": batch_id})
            b = {
                "id": batch_id,
                "stations": cleaned,
                "threshold": threshold,
                "digest": None,        # 由首次有效投票冻结
                "frozen_at": None,
                "votes": [],
                "quarantines": [],     # 审查员隔离裁决（稳定隔离标识 -> 站点 + 原因）
                "certificate": None,
            }
            self.state["batches"][batch_id] = b
            self.state["revision"] += 1
            self._persist_locked()
            return self._snapshot(b, self.state["revision"])

    # ---------- 投票 ----------

    def _find_vote(self, b, station, digest, vote_id, kind=None):
        for v in b["votes"]:
            if v["station"] == station and v["digest"] == digest and v["vote_id"] == vote_id:
                if kind is None or v["kind"] == kind:
                    return v
        return None

    def _find_yes(self, b, station):
        for v in b["votes"]:
            if v["station"] == station and v["kind"] == VOTE_KIND_YES:
                return v
        return None

    def _record_conflict(self, b, station, digest, vote_id, reason, detail):
        """冲突票去重落盘；重传同一冲突载荷只回放。"""
        existing = self._find_vote(b, station, digest, vote_id, VOTE_KIND_CONFLICT)
        replayed = existing is not None
        if existing is None:
            b["votes"].append({
                "id": f"v_{len(b['votes']) + 1}_{hashlib.sha1(os.urandom(16)).hexdigest()[:10]}",
                "station": station,
                "digest": digest,
                "vote_id": vote_id,
                "kind": VOTE_KIND_CONFLICT,
                "reason": reason,
                "ts": utc_now(),
            })
            self.state["revision"] += 1
            self._persist_locked()
        snap = self._snapshot(b, self.state["revision"])
        err = ApiError(
            422, reason,
            f"来自站点 {station} 的投票被隔离：{detail}，不计入赞成票、不参与封存",
            {"replayed": replayed, "batch": snap},
        )
        err.replayed = replayed
        raise err

    @staticmethod
    def _fingerprint(b) -> str:
        quarantined = {q["station"] for q in b.get("quarantines", [])}
        yeses = sorted(
            ((v["station"], v["vote_id"], v["id"]) for v in b["votes"]
             if v["kind"] == VOTE_KIND_YES and v["station"] not in quarantined),
            key=lambda x: x[0],
        )
        material = {
            "batch_id": b["id"],
            "stations": sorted(b["stations"]),
            "threshold": b["threshold"],
            "digest": b["digest"],
            "yes_votes": [{"station": s, "vote_id": vid, "id": i} for s, vid, i in yeses],
        }
        canonical = json.dumps(material, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    def cast_vote(self, batch_id, station, digest, vote_id):
        """返回 (http_status, payload)。整个判定+封存+落盘在同一把锁内完成。"""
        station = _require_str(station, "station", STATION_MAX_LEN)
        digest = _require_str(digest, "digest", DIGEST_MAX_LEN)
        vote_id = _require_str(vote_id, "vote_id", VOTE_ID_MAX_LEN)
        with self.lock:
            b = self.state["batches"].get(batch_id)
            if b is None:
                raise ApiError(404, "BATCH_NOT_FOUND", f"批次 {batch_id} 不存在", {"batch_id": batch_id})

            # 站点不在冻结名单：配置不符，拒绝（不污染冲突记录）。
            if station not in b["stations"]:
                raise ApiError(
                    422, "STATION_NOT_LISTED",
                    f"站点 {station} 不在批次冻结名单中",
                    {"station": station, "frozen_stations": list(b["stations"])},
                )

            quarantined = self._quarantined_map(b)

            # 幂等回放优先于一切状态判定：同站同摘要同投票标识的赞成票，
            # 即使批次已封存也只回放首次结果（“迟到票”仅指未见过的新载荷）。
            # 但回放绝不恢复计票：若该站此后被审查员裁决隔离，有效集合仍将其排除。
            existing_yes = self._find_vote(b, station, digest, vote_id, VOTE_KIND_YES)
            if existing_yes is not None:
                return 200, {
                    "accepted": True,
                    "replayed": True,
                    "vote_id": existing_yes["id"],
                    "quarantined": station in quarantined,
                    "counted": station not in quarantined,
                    "batch": self._snapshot(b, self.state["revision"]),
                }

            # 已记录过的冲突载荷重传：回放首次冲突结果，不重复堆积记录。
            existing_conflict = self._find_vote(b, station, digest, vote_id, VOTE_KIND_CONFLICT)
            if existing_conflict is not None:
                raise ApiError(
                    422, existing_conflict["reason"],
                    f"来自站点 {station} 的冲突票为重传：回放首次隔离结果，不计入赞成票",
                    {"replayed": True, "batch": self._snapshot(b, self.state["revision"])},
                )

            # 已封存且载荷未见过：迟到票一律拒绝，绝不可能改写证书。
            if b["certificate"] is not None:
                raise ApiError(
                    409, "LATE_VOTE_REJECTED",
                    f"批次 {batch_id} 已封存，证书不可变，迟到票被拒绝",
                    {"batch": self._snapshot(b, self.state["revision"])},
                )

            # 审查员裁决隔离的站点：任何后续投票（含“新”载荷）都不得恢复计票。
            if station in quarantined:
                q = quarantined[station]
                raise ApiError(
                    422, "STATION_QUARANTINED",
                    f"站点 {station} 已被审查员裁决隔离（隔离标识 {q['id']}），其投票不计入有效集合",
                    {
                        "station": station,
                        "quarantine_id": q["id"],
                        "quarantine_reason": q["reason"],
                        "batch": self._snapshot(b, self.state["revision"]),
                    },
                )

            # 摘要已冻结且不同：冲突隔离。
            if b["digest"] is not None and digest != b["digest"]:
                self._record_conflict(
                    b, station, digest, vote_id, "DIGEST_MISMATCH",
                    f"摘要 {digest[:16]}… 与冻结摘要 {b['digest'][:16]}… 不一致",
                )

            # 同站对同摘要使用了不同的稳定投票标识：绑定内容改变，冲突隔离。
            prior_yes = self._find_yes(b, station)
            if prior_yes is not None:
                self._record_conflict(
                    b, station, digest, vote_id, "VOTE_ID_CHANGED",
                    f"站点 {station} 已用投票标识 {prior_yes['vote_id']} 投过赞成票，"
                    "同一绑定不得更换投票标识",
                )

            # 首次有效投票：原子冻结摘要（名单与阈值在创建时已固定）。
            frozen_now = False
            if b["digest"] is None:
                b["digest"] = digest
                b["frozen_at"] = utc_now()
                frozen_now = True

            vote = {
                "id": f"v_{len(b['votes']) + 1}_{hashlib.sha1(os.urandom(16)).hexdigest()[:10]}",
                "station": station,
                "digest": digest,
                "vote_id": vote_id,
                "kind": VOTE_KIND_YES,
                "reason": None,
                "ts": utc_now(),
            }
            b["votes"].append(vote)

            snapshot = self._snapshot(b, self.state["revision"] + 1)
            certificate = None
            # 达到阈值的瞬间在同一事务内封签；并发后到者只会看到已封存状态。
            if snapshot["yes_count"] >= b["threshold"]:
                certificate = {
                    "serial": 1,
                    "batch_id": b["id"],
                    "digest": b["digest"],
                    "threshold": b["threshold"],
                    "stations": snapshot["yes_stations"],
                    "fingerprint": self._fingerprint({**b, "votes": b["votes"]}),
                    "sealed_at": utc_now(),
                    "immutable": True,
                }
                b["certificate"] = certificate

            self.state["revision"] += 1
            self._persist_locked()
            return 200, {
                "accepted": True,
                "replayed": False,
                "froze_config": frozen_now,
                "vote_id": vote["id"],
                "batch": self._snapshot(b, self.state["revision"]),
            }

    # ---------- 审查员隔离裁决 ----------

    def quarantine_station(self, batch_id, quarantine_id, station, reason):
        """审查员在封存前提交隔离裁决。返回 (http_status, payload)。

        规则：
          * 仅收集中批次可裁决；封存后的隔离请求一律 409 拒绝，证书逐字节不变。
          * 相同隔离标识 + 相同站点 + 相同原因重传：回放首次裁决结果，不重复变更。
          * 相同隔离标识但站点或原因不同：409 冲突，返回首次裁决内容供核对修正。
          * 被隔离站的既有赞成票立即排出有效集合；有效票不足阈值时批次保持收集中。
        """
        quarantine_id = _require_str(quarantine_id, "quarantine_id", QUARANTINE_ID_MAX_LEN)
        station = _require_str(station, "station", STATION_MAX_LEN)
        reason = _require_str(reason, "reason", QUARANTINE_REASON_MAX_LEN)
        with self.lock:
            b = self.state["batches"].get(batch_id)
            if b is None:
                raise ApiError(404, "BATCH_NOT_FOUND", f"批次 {batch_id} 不存在", {"batch_id": batch_id})

            prior = None
            for q in b.get("quarantines", []):
                if q["id"] == quarantine_id:
                    prior = q
                    break

            # 相同隔离标识：内容一致 => 任何状态下都回放首次结果（封存后亦同），
            # 绝不重复变更；内容不一致 => 可操作冲突（同样不产生新变更）。
            if prior is not None:
                same = prior["station"] == station and prior["reason"] == reason
                if not same:
                    raise ApiError(
                        409, "QUARANTINE_ID_CONFLICT",
                        f"隔离标识 {quarantine_id} 已绑定首次裁决"
                        f"（站点 {prior['station']}，原因：{prior['reason']}），"
                        "不得改用不同站点或原因重传",
                        {
                            "quarantine_id": quarantine_id,
                            "existing": {
                                "station": prior["station"],
                                "reason": prior["reason"],
                                "ts": prior["ts"],
                            },
                            "submitted": {"station": station, "reason": reason},
                            "hint": "请沿用首次裁决的站点与原因重传（将幂等回放），"
                                    "或改用新的隔离标识提交另一项裁决。",
                            "batch": self._snapshot(b, self.state["revision"]),
                        },
                    )
                return 200, {
                    "accepted": True,
                    "replayed": True,
                    "quarantine_id": prior["id"],
                    "station": prior["station"],
                    "reason": prior["reason"],
                    "batch": self._snapshot(b, self.state["revision"]),
                }

            # 封存后的（新）隔离请求必须被拒绝，证书逐字节不变。
            if b["certificate"] is not None:
                cert_bytes = json.dumps(
                    b["certificate"], ensure_ascii=False, sort_keys=True, separators=(",", ":")
                )
                raise ApiError(
                    409, "QUARANTINE_REJECTED_SEALED",
                    f"批次 {batch_id} 已封存，不再受理隔离裁决，证书不可变",
                    {
                        "batch": self._snapshot(b, self.state["revision"]),
                        "certificate_sha256": hashlib.sha256(cert_bytes.encode("utf-8")).hexdigest(),
                    },
                )

            if station not in b["stations"]:
                raise ApiError(
                    422, "STATION_NOT_LISTED",
                    f"站点 {station} 不在批次冻结名单中，无法对其裁决隔离",
                    {"station": station, "frozen_stations": list(b["stations"])},
                )

            record = {
                "id": quarantine_id,
                "station": station,
                "reason": reason,
                "ts": utc_now(),
            }
            b.setdefault("quarantines", []).append(record)
            self.state["revision"] += 1
            self._persist_locked()

            snapshot = self._snapshot(b, self.state["revision"])
            return 200, {
                "accepted": True,
                "replayed": False,
                "quarantine_id": quarantine_id,
                "station": station,
                "reason": reason,
                "batch": snapshot,
            }


STORE = Store(DB_PATH)


class Handler(BaseHTTPRequestHandler):
    server_version = "SealServer/1.0"

    def log_message(self, fmt, *args):
        print(f"[http] {self.address_string()} {fmt % args}", file=sys.stderr)

    # ---------- 工具 ----------

    def _send_json(self, status: int, payload: dict):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self):
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0:
            raise ApiError(400, "EMPTY_BODY", "请求体为空且需要 JSON")
        if length > MAX_BODY_BYTES:
            raise ApiError(413, "BODY_TOO_LARGE", f"请求体超过 {MAX_BODY_BYTES} 字节")
        raw = self.rfile.read(length)
        try:
            data = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ApiError(400, "INVALID_JSON", f"JSON 解析失败: {exc}")
        if not isinstance(data, dict):
            raise ApiError(422, "INVALID_BODY", "请求体必须是 JSON 对象")
        return data

    def _api_error(self, err: ApiError):
        self._send_json(err.status, err.body())

    def _serve_index(self):
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "index.html")
        try:
            with open(path, "rb") as f:
                body = f.read()
        except OSError:
            self._send_json(500, {"error": {"code": "INDEX_MISSING", "message": "页面文件缺失"}})
            return
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    # ---------- 路由 ----------

    def do_GET(self):
        try:
            parsed = urlparse(self.path)
            path = parsed.path.rstrip("/") or "/"
            if path == "/health":
                self._send_json(200, {"status": "ok", "revision": STORE.state["revision"]})
            elif path == "/":
                self._serve_index()
            elif path == "/api/batches":
                self._send_json(200, {"batches": STORE.list_snapshots()})
            elif path.startswith("/api/batches/"):
                batch_id = unquote(path[len("/api/batches/"):])
                self._send_json(200, {"batch": STORE.get_batch_snapshot(batch_id)})
            else:
                self._send_json(404, {"error": {"code": "NOT_FOUND", "message": path}})
        except ApiError as err:
            self._api_error(err)
        except Exception:  # noqa: BLE001
            traceback.print_exc()
            self._send_json(500, {"error": {"code": "INTERNAL", "message": "服务内部错误"}})

    def do_POST(self):
        try:
            parsed = urlparse(self.path)
            path = parsed.path.rstrip("/")
            if path == "/api/batches":
                data = self._read_json()
                snap = STORE.create_batch(data.get("id"), data.get("stations"), data.get("threshold"))
                self._send_json(201, {"batch": snap})
            elif path.startswith("/api/batches/") and path.endswith("/votes"):
                batch_id = unquote(path[len("/api/batches/"):-len("/votes")])
                data = self._read_json()
                status, payload = STORE.cast_vote(
                    batch_id, data.get("station"), data.get("digest"), data.get("vote_id")
                )
                self._send_json(status, payload)
            elif path.startswith("/api/batches/") and path.endswith("/quarantine"):
                batch_id = unquote(path[len("/api/batches/"):-len("/quarantine")])
                data = self._read_json()
                status, payload = STORE.quarantine_station(
                    batch_id, data.get("quarantine_id"), data.get("station"), data.get("reason")
                )
                self._send_json(status, payload)
            else:
                self._send_json(404, {"error": {"code": "NOT_FOUND", "message": path}})
        except ApiError as err:
            self._api_error(err)
        except Exception:  # noqa: BLE001
            traceback.print_exc()
            self._send_json(500, {"error": {"code": "INTERNAL", "message": "服务内部错误"}})


def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    httpd = ThreadingHTTPServer((BIND_HOST, BIND_PORT), Handler)
    httpd.daemon_threads = True
    print(f"[boot] 封存服务监听 {BIND_HOST}:{BIND_PORT}，数据文件 {DB_PATH}", file=sys.stderr)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()


if __name__ == "__main__":
    main()
