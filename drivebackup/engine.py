#!/usr/bin/env python3
import base64
import hashlib
import io
import json
import os
import queue
import re
import socket
import ssl
import stat
import struct
import subprocess
import sys
import tarfile
import threading
import time
import urllib.parse
import urllib.request
from collections import deque, namedtuple
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from types import SimpleNamespace

STATE = os.environ.get("STATE_DIR", "/data")
REMOTE = os.environ.get("DRIVE_REMOTE", "gdrive")
ROOT = os.environ.get("DRIVE_ROOT", "MinecraftBackups/server").strip("/")
DAILY = "%s:%s/daily" % (REMOTE, ROOT)
KEEP = int(os.environ.get("KEEP", "7"))
CHUNK = os.environ.get("DRIVE_CHUNK", "32M")
SOURCE_TYPE = os.environ.get("SOURCE_TYPE", "local")
SOURCE_PATH = os.environ.get("SOURCE_PATH", "/source")
STREAMS = int(os.environ.get("STREAMS", "1"))
BWLIMIT_MB = float(os.environ.get("BWLIMIT_MB", "0"))
SMALL_LIMIT = int(os.environ.get("SMALL_LIMIT", str(64 << 20)))
RACY_SECONDS = int(os.environ.get("RACY_SECONDS", "7"))
STALL_SECONDS = int(os.environ.get("STALL_SECONDS", "900"))
DEADLINE_SECONDS = int(os.environ.get("DEADLINE_SECONDS", "72000"))
PART_ATTEMPTS = int(os.environ.get("PART_ATTEMPTS", "3"))
DB_ATTEMPTS = int(os.environ.get("DB_ATTEMPTS", "2"))
PREFETCH_REQUESTS = int(os.environ.get("PREFETCH_REQUESTS", "64"))
READERS = int(os.environ.get("READERS", "4"))
WINDOW_BYTES = int(os.environ.get("WINDOW_BYTES", str(96 << 20)))
WINDOW_FILES = int(os.environ.get("WINDOW_FILES", "128"))
CONNECT_ATTEMPTS = int(os.environ.get("CONNECT_ATTEMPTS", "5"))
SIZE_ESTIMATE = int(os.environ.get("SIZE_ESTIMATE", str(10 * 10 ** 9)))
RCLONE_SOURCE = os.environ.get("RCLONE_SOURCE", "/run/secrets/rclone.conf")
EXCLUDES = tuple(x for x in os.environ.get(
    "EXCLUDES", "logs,cache,.cache,crash-reports,debug,plugins/.paper-remapped,world/session.lock").split(",") if x)
ONLY = tuple(x for x in os.environ.get("ONLY", "").split(",") if x)
ZSTD_FILES = os.environ.get("ZSTD_FILES", "--fast=1")
ZSTD_DB = os.environ.get("ZSTD_DB", "-1")
RCON_HOST = os.environ.get("RCON_HOST", "")
RCON_PORT = int(os.environ.get("RCON_PORT", "25575"))
SAVE_COMMAND = os.environ.get("SAVE_COMMAND", "save-all flush")
SAVE_TIMEOUT = int(os.environ.get("SAVE_TIMEOUT", "900"))
MARIADB_HOST = os.environ.get("MARIADB_HOST", "")
MARIADB_PORT = os.environ.get("MARIADB_PORT", "3306")
MARIADB_USER = os.environ.get("MARIADB_USER", "")
MARIADB_DATABASES = tuple(x for x in os.environ.get("MARIADB_DATABASES", "").split(",") if x)
MARIADB_SSL = os.environ.get("MARIADB_SSL", "off")
MARIADB_SSL_CA = os.environ.get("MARIADB_SSL_CA", "")
CLICKHOUSE_URL = os.environ.get("CLICKHOUSE_URL", "").rstrip("/")
CLICKHOUSE_DATABASE = os.environ.get("CLICKHOUSE_DATABASE", "default")
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "default")
CLICKHOUSE_TLS_VERIFY = os.environ.get("CLICKHOUSE_TLS_VERIFY", "1") != "0"
PEER_PATHS = tuple(x for x in os.environ.get("PEER_PATHS", "").split(",") if x)
PEER_WAIT = int(os.environ.get("PEER_WAIT", "3600"))
PEER_MAX_AGE = int(os.environ.get("PEER_MAX_AGE", "21600"))
PEER_POLL = int(os.environ.get("PEER_POLL", "60"))
SERVE_POLL = int(os.environ.get("SERVE_POLL", "5"))
CONF = "/tmp/rclone.conf"
BUF = 1 << 20
FINAL_RE = re.compile(r"^\d{4}-\d{2}-\d{2}_\d{4}$")
SYSTEM_DBS = {"information_schema", "performance_schema", "mysql", "sys"}
DERIVED_TABLES = ("View", "MaterializedView", "LiveView", "Dictionary")
RCLONE = ["rclone", "--config", CONF, "--low-level-retries", "20", "--log-level", "NOTICE"]

Entry = namedtuple("Entry", "kind size mtime mode")
Fetched = namedtuple("Fetched", "data st started stable")
TooBig = namedtuple("TooBig", "st")

_logf = None


class Abort(Exception):
    pass


def now():
    return datetime.now().astimezone()


def log(msg):
    line = "%s %s" % (now().strftime("%Y-%m-%d %H:%M:%S"), msg)
    print(line, flush=True)
    if _logf:
        _logf.write(line + "\n")
        _logf.flush()


class Status:
    def __init__(self):
        self.path = os.path.join(STATE, "status.json")
        self.data = {}
        self.lock = threading.Lock()

    def load(self):
        try:
            with open(self.path, encoding="utf-8") as f:
                self.data = json.load(f)
        except (FileNotFoundError, ValueError):
            self.data = {}
        return self.data

    def set(self, **kw):
        with self.lock:
            self.data.update(kw)
            tmp = self.path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self.data, f, ensure_ascii=False)
            os.replace(tmp, self.path)

    def archive(self):
        path = os.path.join(STATE, "history.jsonl")
        try:
            with open(path, encoding="utf-8") as f:
                lines = f.read().splitlines()[-99:]
        except FileNotFoundError:
            lines = []
        lines.append(json.dumps(self.data, ensure_ascii=False))
        with open(path + ".tmp", "w", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")
        os.replace(path + ".tmp", path)


def drain(stream, tag):
    def run():
        for raw in iter(stream.readline, b""):
            text = raw.decode("utf-8", "replace").rstrip()
            if text:
                log("%s: %s" % (tag, text))
        stream.close()
    t = threading.Thread(target=run, daemon=True)
    t.start()
    return t


def rclone(*args, timeout=1800):
    p = subprocess.run(RCLONE + list(args), stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)
    if p.returncode:
        raise RuntimeError("rclone %s: %s" % (args[0], p.stderr.decode("utf-8", "replace").strip()[-400:]))
    return p.stdout.decode("utf-8", "replace")


def gdrive_section(text):
    lines, inside = [], False
    for line in text.splitlines():
        if line.startswith("["):
            inside = line.strip() == "[%s]" % REMOTE
        if inside:
            lines.append(line)
    if not lines:
        raise RuntimeError("no [%s] section in rclone config" % REMOTE)
    return "\n".join(lines) + "\n"


def load_token():
    with open(RCLONE_SOURCE, encoding="utf-8") as f:
        section = gdrive_section(f.read())
    old = os.umask(0o077)
    try:
        with open(CONF, "w", encoding="utf-8") as f:
            f.write(section)
    finally:
        os.umask(old)


def connect_once():
    import paramiko
    host, port = os.environ["SFTP_HOST"], int(os.environ.get("SFTP_PORT", "22"))
    t = paramiko.Transport(socket.create_connection((host, port), timeout=30))
    t.start_client(timeout=30)
    pinned = os.environ.get("SFTP_HOSTKEY", "")
    if pinned:
        digest = base64.b64encode(hashlib.sha256(t.get_remote_server_key().asbytes()).digest()).decode().rstrip("=")
        if "SHA256:%s" % digest != pinned:
            t.close()
            raise RuntimeError("SFTP host key mismatch: SHA256:%s" % digest)
    key_file = os.environ.get("SFTP_KEY_FILE", "")
    if key_file:
        t.auth_publickey(os.environ["SFTP_USER"], paramiko.Ed25519Key.from_private_key_file(key_file))
    else:
        t.auth_password(os.environ["SFTP_USER"], os.environ["SFTP_PASSWORD"])
    client = paramiko.SFTPClient.from_transport(t, window_size=1 << 24, max_packet_size=32768)
    client.get_channel().settimeout(120)
    return client


def sftp_connect():
    import paramiko
    last = None
    for n in range(1, CONNECT_ATTEMPTS + 1):
        try:
            return connect_once()
        except paramiko.AuthenticationException:
            raise
        except (OSError, paramiko.SSHException) as e:
            last = e
            log("SFTP connect %d of %d failed: %s" % (n, CONNECT_ATTEMPTS, str(e)[:200]))
            if n < CONNECT_ATTEMPTS:
                time.sleep(10 * n)
    raise RuntimeError("SFTP connect failed: %s" % str(last)[:200])


class LocalFile:
    def __init__(self, path, mode="rb"):
        self.f = open(path, mode)

    def prefetch(self, *_):
        pass

    def set_pipelined(self, *_):
        pass

    def write(self, data):
        return self.f.write(data)

    def read(self, n=-1):
        return self.f.read(n)

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.f.close()


class LocalFS:
    def __init__(self, root):
        self.root = root.rstrip("/")

    def path(self, rel):
        if not self.root:
            return rel
        return self.root + "/" + rel.lstrip("/") if rel.strip("/") else self.root

    def listdir_attr(self, rel):
        out = []
        try:
            for name in os.listdir(self.path(rel)):
                try:
                    st = os.lstat(os.path.join(self.path(rel), name))
                except OSError:
                    continue
                out.append(SimpleNamespace(filename=name, st_mode=st.st_mode, st_size=st.st_size, st_mtime=st.st_mtime))
        except PermissionError:
            log("no access to %s, skipped" % self.path(rel))
        return out

    def open(self, rel, mode="rb"):
        return LocalFile(self.path(rel), mode)

    def mkdir(self, rel, mode=0o755):
        os.mkdir(self.path(rel), mode)

    def chmod(self, rel, mode):
        os.chmod(self.path(rel), mode)

    def remove(self, rel):
        os.remove(self.path(rel))

    def symlink(self, target, rel):
        os.symlink(target, self.path(rel))

    def utime(self, rel, times):
        os.utime(self.path(rel), times)

    def stat(self, rel):
        return os.stat(self.path(rel))

    def readlink(self, rel):
        return os.readlink(self.path(rel))


def source_connect():
    if SOURCE_TYPE == "local":
        return LocalFS(SOURCE_PATH)
    if SOURCE_TYPE == "sftp":
        return sftp_connect()
    raise RuntimeError("SOURCE_TYPE must be local or sftp, not %s" % SOURCE_TYPE)


def sftp_close(sftp):
    try:
        transport = sftp.get_channel().get_transport()
        sftp.close()
        transport.close()
    except Exception:
        pass


def matches(rel, prefix):
    return rel == prefix or rel.startswith(prefix + "/")


def wanted(rel, is_dir, excludes=None, only=None):
    excludes = EXCLUDES if excludes is None else excludes
    only = ONLY if only is None else only
    if any(matches(rel, e) for e in excludes):
        return False
    if not only:
        return True
    return any(matches(rel, p) or (is_dir and p.startswith(rel + "/")) for p in only)


def walk(sftp, excludes=None, only=None):
    entries, stack = {}, [""]
    while stack:
        folder = stack.pop()
        try:
            listing = sftp.listdir_attr("/" + folder)
        except FileNotFoundError:
            continue
        for a in listing:
            rel = "%s/%s" % (folder, a.filename) if folder else a.filename
            mode = a.st_mode or 0
            is_dir = stat.S_ISDIR(mode)
            if not wanted(rel, is_dir, excludes, only):
                continue
            if is_dir:
                entries[rel] = Entry("d", 0, int(a.st_mtime or 0), stat.S_IMODE(mode))
                stack.append(rel)
            elif stat.S_ISLNK(mode):
                entries[rel] = Entry("l", 0, int(a.st_mtime or 0), stat.S_IMODE(mode))
            elif stat.S_ISREG(mode):
                entries[rel] = Entry("f", int(a.st_size or 0), int(a.st_mtime or 0), stat.S_IMODE(mode))
    return entries


def clean(name):
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", name)


def part_of(rel):
    parts = rel.split("/")
    if parts[0].startswith("world_"):
        return "world-%s" % clean(parts[0][len("world_"):])
    if parts[0] != "world":
        return "server-files"
    if len(parts) > 1 and parts[1] == "DIM-1":
        return "world-nether"
    if len(parts) > 1 and parts[1] == "DIM1":
        return "world-end"
    if len(parts) >= 4 and parts[1] == "dimensions":
        return "world-%s-%s" % (clean(parts[2]), clean(parts[3]))
    return "world-overworld"


def plan_parts(entries):
    plan = {}
    for rel in sorted(entries):
        plan.setdefault(part_of(rel), []).append(rel)
    return dict(sorted(plan.items(), key=lambda kv: (kv[0] != "server-files", kv[0])))


class Attempt:
    def __init__(self, name):
        self.name = name
        self.read = 0
        self.uploaded = 0
        self.last = time.monotonic()
        self.procs = []
        self.conns = []
        self.killed = None

    def touch(self):
        self.last = time.monotonic()

    def close_conns(self):
        for c in self.conns:
            sftp_close(c)

    def kill(self, reason):
        self.killed = reason
        for p in self.procs:
            if p.poll() is None:
                p.kill()
        self.close_conns()


class Tracker:
    def __init__(self, st, total):
        self.st = st
        self.total = total
        self.lock = threading.Lock()
        self.done_read = 0
        self.done_up = 0
        self.active = {}
        self.captured = {}
        self.dirty = set()
        self.vanished = set()
        self.stop = threading.Event()
        self.t0 = time.monotonic()
        self.deadline_hit = False

    def begin(self, attempt):
        with self.lock:
            self.active[attempt.name] = attempt

    def finish(self, attempt, keep):
        with self.lock:
            self.active.pop(attempt.name, None)
            if keep:
                self.done_read += attempt.read
                self.done_up += attempt.uploaded

    def record(self, rel, size, mtime, started, dirty):
        with self.lock:
            self.captured[rel] = (size, mtime)
            if dirty or abs(started - mtime) <= RACY_SECONDS:
                self.dirty.add(rel)
            else:
                self.dirty.discard(rel)

    def totals(self):
        with self.lock:
            return (self.done_read + sum(a.read for a in self.active.values()),
                    self.done_up + sum(a.uploaded for a in self.active.values()))

    def monitor(self):
        prev_read, prev_t = 0, time.monotonic()
        rate = 0.0
        while not self.stop.wait(15):
            read, up = self.totals()
            t = time.monotonic()
            if t > prev_t:
                sample = max(read - prev_read, 0) / (t - prev_t)
                rate = sample if rate == 0 else rate * 0.8 + sample * 0.2
            prev_read, prev_t = read, t
            eta = int((self.total - read) / rate) if rate > 0 and self.total > read else 0
            self.st.set(read_bytes=read, up_bytes=up, total_bytes=self.total, rate=round(rate), eta=eta)
            with self.lock:
                active = list(self.active.values())
            for a in active:
                if t - a.last > STALL_SECONDS:
                    log("%s: no data for %d s, killing the stream" % (a.name, STALL_SECONDS))
                    a.kill("stalled")
            if t - self.t0 > DEADLINE_SECONDS:
                self.deadline_hit = True
                for a in active:
                    a.kill("run limit reached")


class Exact:
    def __init__(self, f, size, attempt):
        self.f, self.left, self.attempt = f, size, attempt
        self.short = False

    def read(self, n=-1):
        n = self.left if n < 0 else min(n, self.left)
        out = bytearray()
        while len(out) < n and not self.short:
            chunk = self.f.read(n - len(out))
            if not chunk:
                self.short = True
                break
            out += chunk
        if len(out) < n:
            out += b"\0" * (n - len(out))
        self.left -= n
        self.attempt.read += n
        self.attempt.touch()
        return bytes(out)


def read_exact(f, size):
    if size > 32768:
        f.prefetch(size, PREFETCH_REQUESTS)
    out = bytearray()
    while len(out) < size:
        chunk = f.read(min(BUF, size - len(out)))
        if not chunk:
            break
        out += chunk
    return bytes(out)


def tar_info(rel, size, mtime, mode, kind=tarfile.REGTYPE):
    ti = tarfile.TarInfo(rel)
    ti.size, ti.mtime, ti.mode, ti.type = size, mtime, mode or 0o644, kind
    return ti


def add_big(tf, sftp, rel, st0, tracker, attempt):
    started = time.time()
    with sftp.open("/" + rel, "rb") as f:
        f.prefetch(st0.st_size, PREFETCH_REQUESTS)
        reader = Exact(f, st0.st_size, attempt)
        tf.addfile(tar_info(rel, st0.st_size, int(st0.st_mtime), stat.S_IMODE(st0.st_mode or 0o644)), reader)
    st1 = sftp.stat("/" + rel)
    dirty = reader.short or st1.st_size != st0.st_size or int(st1.st_mtime) != int(st0.st_mtime)
    tracker.record(rel, st0.st_size, int(st0.st_mtime), started, dirty)


def read_stable(sftp, rel, e, attempt):
    path = "/" + rel
    size, mtime = e.size, e.mtime
    data, st1, started, stable = b"", None, 0.0, False
    for _ in range(3):
        started = time.time()
        try:
            with sftp.open(path, "rb") as f:
                data = read_exact(f, size)
            st1 = sftp.stat(path)
        except FileNotFoundError:
            return None
        attempt.touch()
        stable = st1.st_size == size == len(data) and int(st1.st_mtime) == mtime
        if stable:
            break
        if st1.st_size > SMALL_LIMIT:
            return TooBig(st1)
        size, mtime = st1.st_size, int(st1.st_mtime)
    return Fetched(data, st1, started, stable)


def write_tar(w, sftp, rels, entries, tracker, attempt, readers=None):
    tf = tarfile.open(fileobj=w, mode="w|", format=tarfile.GNU_FORMAT, bufsize=BUF)
    idle = queue.Queue()
    for conn in readers or [sftp]:
        idle.put(conn)

    def fetch(rel, e):
        conn = idle.get()
        try:
            return read_stable(conn, rel, e, attempt)
        finally:
            idle.put(conn)

    pending = deque()
    files = dirs = pending_bytes = 0
    executor = ThreadPoolExecutor(max_workers=idle.qsize())

    def flush_one():
        nonlocal files, pending_bytes
        rel, size, future = pending.popleft()
        got = future.result()
        pending_bytes -= size
        if got is None:
            tracker.vanished.add(rel)
        elif isinstance(got, TooBig):
            add_big(tf, sftp, rel, got.st, tracker, attempt)
            files += 1
        else:
            mode = stat.S_IMODE(got.st.st_mode or 0o644)
            tf.addfile(tar_info(rel, len(got.data), int(got.st.st_mtime), mode), io.BytesIO(got.data))
            attempt.read += len(got.data)
            tracker.record(rel, len(got.data), int(got.st.st_mtime), got.started, not got.stable)
            files += 1
        attempt.touch()

    try:
        for rel in rels:
            e = entries[rel]
            if e.kind == "d":
                tf.addfile(tar_info(rel, 0, e.mtime, e.mode or 0o755, tarfile.DIRTYPE))
                dirs += 1
        for rel in rels:
            e = entries[rel]
            if e.kind == "d":
                continue
            if e.kind == "f" and e.size <= SMALL_LIMIT:
                while pending and (pending_bytes + e.size > WINDOW_BYTES or len(pending) >= WINDOW_FILES):
                    flush_one()
                pending.append((rel, e.size, executor.submit(fetch, rel, e)))
                pending_bytes += e.size
                continue
            while pending:
                flush_one()
            try:
                if e.kind == "l":
                    ti = tar_info(rel, 0, e.mtime, 0o777, tarfile.SYMTYPE)
                    ti.linkname = sftp.readlink("/" + rel) or ""
                    tf.addfile(ti)
                else:
                    add_big(tf, sftp, rel, sftp.stat("/" + rel), tracker, attempt)
                    files += 1
            except FileNotFoundError:
                tracker.vanished.add(rel)
            attempt.touch()
        while pending:
            flush_one()
        tf.close()
    finally:
        executor.shutdown(wait=False, cancel_futures=True)
    return files, dirs


def bw_arg():
    return "%.2fM" % (BWLIMIT_MB / max(STREAMS, 1) / 1.048576)


def upload(part, rel, attempt, cmd=None, feed=None, env=None, bwlimit=True, level=ZSTD_DB):
    dest = "%s/%s" % (part, rel)
    log("upload %s" % rel)
    t0 = time.monotonic()
    src = None
    if cmd:
        src = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
        drain(src.stderr, os.path.basename(cmd[0]))
        attempt.procs.append(src)
    zst = subprocess.Popen(["zstd", level, "-T1", "-q", "-c"],
                           stdin=src.stdout if src else subprocess.PIPE, stdout=subprocess.PIPE)
    attempt.procs.append(zst)
    if src:
        src.stdout.close()
    args = ["rcat", "--drive-chunk-size", CHUNK]
    if bwlimit and BWLIMIT_MB > 0:
        args += ["--bwlimit", bw_arg()]
    rc = subprocess.Popen(RCLONE + args + [dest], stdin=subprocess.PIPE, stderr=subprocess.PIPE)
    attempt.procs.append(rc)
    drain(rc.stderr, "rclone")
    errors, result, feeder = [], {}, None
    if feed:
        def pump():
            try:
                result.update(feed(zst.stdin))
            except Exception as e:
                errors.append(e)
            finally:
                try:
                    zst.stdin.close()
                except OSError:
                    pass
        feeder = threading.Thread(target=pump, daemon=True)
        feeder.start()
    md5 = hashlib.md5()
    size = 0
    try:
        while True:
            chunk = zst.stdout.read(BUF)
            if not chunk:
                break
            md5.update(chunk)
            size += len(chunk)
            attempt.uploaded = size
            attempt.touch()
            rc.stdin.write(chunk)
        rc.stdin.close()
        src_code = src.wait() if src else 0
        if feeder:
            feeder.join()
        zst_code, rc_code = zst.wait(), rc.wait()
        if errors:
            raise RuntimeError("%s: source failed: %s" % (rel, errors[0]))
        if attempt.killed:
            raise RuntimeError("%s: %s" % (rel, attempt.killed))
        if src_code:
            raise RuntimeError("%s: %s exited %d" % (rel, cmd[0], src_code))
        if zst_code or rc_code:
            raise RuntimeError("%s: zstd=%d rclone=%d" % (rel, zst_code, rc_code))
        local = md5.hexdigest()
        remote = (rclone("md5sum", dest).split() or [""])[0]
        if remote != local:
            raise RuntimeError("%s: md5 mismatch local=%s drive=%s" % (rel, local, remote))
    except BaseException:
        for p in attempt.procs:
            if p.poll() is None:
                p.kill()
        try:
            rclone("deletefile", dest, timeout=300)
            log("removed incomplete %s" % rel)
        except Exception as e:
            log("nothing removed for %s: %s" % (rel, str(e)[:200]))
        raise
    secs = time.monotonic() - t0
    log("ok %s: %d bytes, %.0f s, md5 %s" % (rel, size, secs, local))
    out = {"name": rel, "size": size, "md5": local, "seconds": round(secs)}
    out.update(result)
    return out


def stream_part(part, name, rels, entries, tracker, connect=None):
    connect = connect or source_connect
    rel = "%s.tar.zst" % name
    last = None
    for n in range(1, PART_ATTEMPTS + 1):
        attempt = Attempt(name)
        tracker.begin(attempt)
        keep = False
        try:
            for _ in range(max(1, min(READERS, len(rels)))):
                attempt.conns.append(connect())

            def feed(w):
                files, dirs = write_tar(w, attempt.conns[0], rels, entries, tracker, attempt, attempt.conns)
                return {"files": files, "dirs": dirs, "raw_bytes": attempt.read}

            item = upload(part, rel, attempt, feed=feed, level=ZSTD_FILES)
            keep = True
            return item
        except Exception as e:
            last = e
            log("%s: attempt %d of %d failed: %s" % (name, n, PART_ATTEMPTS, str(e)[:300]))
            if tracker.deadline_hit:
                raise Abort("run limit reached")
        finally:
            tracker.finish(attempt, keep)
            attempt.close_conns()
        time.sleep(min(30 * n, 120))
    raise RuntimeError("%s failed after %d attempts: %s" % (name, PART_ATTEMPTS, str(last)[:300]))


class Rcon:
    def __init__(self, host, port, password, timeout=30):
        self.sock = socket.create_connection((host, port), timeout)
        self.rid = 0
        rid, _ = self._request(3, password, timeout)
        if rid == -1:
            self.close()
            raise RuntimeError("RCON auth failed")

    def _read(self, n):
        data = b""
        while len(data) < n:
            part = self.sock.recv(n - len(data))
            if not part:
                raise ConnectionError("RCON connection closed")
            data += part
        return data

    def _request(self, kind, body, timeout):
        self.rid += 1
        payload = struct.pack("<ii", self.rid, kind) + body.encode("utf-8") + b"\x00\x00"
        self.sock.settimeout(timeout)
        self.sock.sendall(struct.pack("<i", len(payload)) + payload)
        size = struct.unpack("<i", self._read(4))[0]
        data = self._read(size)
        rid, _ = struct.unpack("<ii", data[:8])
        return rid, data[8:-2].decode("utf-8", "replace")

    def cmd(self, command, timeout=60):
        return self._request(2, command, timeout)[1].strip()

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass


def save_world(problems, label):
    if not RCON_HOST:
        return
    try:
        r = Rcon(RCON_HOST, RCON_PORT, os.environ.get("RCON_PASSWORD", ""))
        try:
            log("%s %s: %s" % (SAVE_COMMAND, label, r.cmd(SAVE_COMMAND, timeout=SAVE_TIMEOUT)))
        finally:
            r.close()
    except Exception as e:
        log("%s %s: %s" % (SAVE_COMMAND, label, e))
        problems.append("%s %s: %s" % (SAVE_COMMAND, label, str(e)[:200]))


def changed_files(captured, dirty, second):
    changed = []
    for rel, e in second.items():
        if e.kind != "f":
            continue
        got = captured.get(rel)
        if got is None or got != (e.size, e.mtime) or rel in dirty:
            changed.append(rel)
    return sorted(changed)


def new_nonfiles(first, second):
    return sorted(rel for rel, e in second.items() if e.kind != "f" and rel not in first)


def deleted_paths(first, second):
    return sorted(rel for rel in first if rel not in second)


def run_delta(part, st, entries_first, tracker, problems, notes):
    sftp = source_connect()
    try:
        st.set(stage="save2")
        save_world(problems, "before delta")
        st.set(stage="walk2")
        second = walk(sftp)
    finally:
        sftp_close(sftp)
    changed = changed_files(tracker.captured, tracker.dirty, second)
    rels = sorted(new_nonfiles(entries_first, second) + changed)
    deleted = deleted_paths(entries_first, second)
    size = sum(second[r].size for r in changed)
    log("delta: %d files, %d bytes, %d deleted" % (len(changed), size, len(deleted)))
    tracker.total += size
    tracker.dirty.clear()
    st.set(stage="delta", delta_files=len(changed), delta_bytes=size, total_bytes=tracker.total)
    item = stream_part(part, "delta", rels, second, tracker)
    item["delta"] = True
    if tracker.dirty:
        notes.append("%d files were still changing during the delta" % len(tracker.dirty))
    return item, deleted


def retrying(label, action, sleep=time.sleep):
    for n in range(1, DB_ATTEMPTS + 1):
        try:
            return action()
        except Exception as e:
            log("%s: attempt %d of %d failed: %s" % (label, n, DB_ATTEMPTS, str(e)[:300]))
            if n == DB_ATTEMPTS:
                raise
            sleep(30 * n)


def db_env():
    return dict(os.environ, MYSQL_PWD=os.environ.get("MARIADB_PASSWORD", ""))


def db_args():
    args = ["-h", MARIADB_HOST, "-P", MARIADB_PORT, "-u", MARIADB_USER]
    if MARIADB_SSL_CA:
        return args + ["--ssl", "--ssl-ca=%s" % MARIADB_SSL_CA]
    return args + (["--ssl", "--skip-ssl-verify-server-cert"] if MARIADB_SSL == "on" else ["--skip-ssl"])


def db_names():
    if MARIADB_DATABASES:
        return list(MARIADB_DATABASES)
    p = subprocess.run(["mariadb"] + db_args() + ["-N", "-B", "-e", "SHOW DATABASES"],
                       env=db_env(), stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=60)
    if p.returncode:
        raise RuntimeError("cannot list databases: %s" % p.stderr.decode("utf-8", "replace").strip()[-200:])
    names = [n for n in p.stdout.decode("utf-8").split() if n not in SYSTEM_DBS]
    if not names:
        raise RuntimeError("database list is empty")
    return names


def dump_cmd(db):
    return ["mariadb-dump"] + db_args() + ["--single-transaction", "--quick", "--no-tablespaces", "--triggers",
                                           "--routines", "--events", "--hex-blob", "--default-character-set=utf8mb4",
                                           "--max-allowed-packet=1G", "--databases", db]


def tracked_upload(part, rel, tracker, name, **kw):
    attempt = Attempt(name)
    tracker.begin(attempt)
    try:
        return upload(part, rel, attempt, **kw)
    finally:
        tracker.finish(attempt, False)


def dump_databases(part, st, tracker, items, problems):
    if not MARIADB_HOST:
        return
    try:
        names = db_names()
    except Exception as e:
        log("MariaDB: %s" % e)
        problems.append("MariaDB: %s" % str(e)[:300])
        return
    for db in names:
        st.set(stage="MariaDB %s" % db)
        try:
            items.append(retrying("MariaDB %s" % db, lambda: tracked_upload(
                part, "mariadb/%s.sql.zst" % db, tracker, "db-" + db, cmd=dump_cmd(db), env=db_env())))
        except Exception as e:
            log("MariaDB %s failed: %s" % (db, e))
            problems.append("MariaDB %s: %s" % (db, str(e)[:300]))


class ClickHouse:
    def __init__(self):
        self.url = CLICKHOUSE_URL + "/"
        self.headers = {"X-ClickHouse-User": CLICKHOUSE_USER,
                        "X-ClickHouse-Key": os.environ.get("CLICKHOUSE_PASSWORD", "")}
        self.ctx = None
        if self.url.startswith("https"):
            self.ctx = ssl.create_default_context()
            if not CLICKHOUSE_TLS_VERIFY:
                self.ctx.check_hostname = False
                self.ctx.verify_mode = ssl.CERT_NONE

    def open(self, query):
        params = urllib.parse.urlencode({"database": CLICKHOUSE_DATABASE, "max_threads": "1", "max_block_size": "65536",
                                         "max_execution_time": "0", "http_send_timeout": "1800"})
        req = urllib.request.Request(self.url + "?" + params, data=query.encode("utf-8"), headers=self.headers)
        return urllib.request.urlopen(req, timeout=600, context=self.ctx)

    def text(self, query):
        with self.open(query) as r:
            return r.read().decode("utf-8")

    def feeder(self, query):
        def feed(w):
            with self.open(query) as r:
                while True:
                    chunk = r.read(BUF)
                    if not chunk:
                        break
                    w.write(chunk)
            return {}
        return feed


def split_tables(rows):
    tables = [name for name, engine in rows if engine not in DERIVED_TABLES]
    views = [name for name, engine in rows if engine in DERIVED_TABLES]
    return tables, views


def dump_clickhouse(part, st, tracker, items, problems):
    if not CLICKHOUSE_URL:
        return
    try:
        ch = ClickHouse()
        rows = [line.split("\t") for line in ch.text(
            "SELECT name, engine FROM system.tables WHERE database = currentDatabase() ORDER BY name FORMAT TSV"
        ).splitlines() if line]
        tables, views = split_tables(rows)
        schema = "".join(ch.text("SHOW CREATE TABLE `%s`.`%s` FORMAT TSVRaw" % (CLICKHOUSE_DATABASE, t)).rstrip() + ";\n\n"
                         for t in tables + views).encode("utf-8")

        def feed_schema(w):
            w.write(schema)
            return {}

        items.append(tracked_upload(part, "clickhouse/schema.sql.zst", tracker, "ch-schema", feed=feed_schema))
        for t in tables:
            st.set(stage="ClickHouse %s" % t)
            count = int(ch.text("SELECT count() FROM `%s`.`%s`" % (CLICKHOUSE_DATABASE, t)).strip() or 0)
            item = retrying("ClickHouse %s" % t, lambda: tracked_upload(
                part, "clickhouse/%s.native.zst" % t, tracker, "ch-" + t,
                feed=ch.feeder("SELECT * FROM `%s`.`%s` FORMAT Native" % (CLICKHOUSE_DATABASE, t))))
            item["rows"] = count
            items.append(item)
    except Exception as e:
        log("ClickHouse failed: %s" % e)
        problems.append("ClickHouse: %s" % str(e)[:300])


def stamped(entries):
    return sorted(e["Name"] for e in entries if FINAL_RE.match(e["Name"]))


def list_daily():
    try:
        return json.loads(rclone("lsjson", "--dirs-only", DAILY) or "[]")
    except RuntimeError as e:
        if "directory not found" in str(e):
            return []
        raise


def purge(name):
    rclone("purge", "--drive-use-trash=false", "%s/%s" % (DAILY, name))


def drive_free():
    return int(json.loads(rclone("about", "--json", "gdrive:", timeout=120)).get("free", 0))


def preflight(st, problems, list_fn=list_daily, free_fn=drive_free, purge_fn=purge):
    names = stamped(list_fn())
    last = Status().load().get("last_good_size") or SIZE_ESTIMATE
    need = int(last * 1.1)
    free = free_fn()
    log("Drive free %d bytes, need %d, copies %d" % (free, need, len(names)))
    if free >= need:
        return
    if len(names) < KEEP:
        raise RuntimeError("Drive has %d GB free, needs %d GB, only %d copies to free it" % (
            free // 10 ** 9, need // 10 ** 9, len(names)))
    log("too little room, deleting oldest copy %s first" % names[0])
    purge_fn(names[0])
    problems.append("Too little room on Drive: oldest copy %s deleted before upload" % names[0])
    free = free_fn()
    if free < need:
        raise RuntimeError("Drive has %d GB free after deleting the oldest copy, needs %d GB" % (
            free // 10 ** 9, need // 10 ** 9))


def retention(current, list_fn=list_daily, purge_fn=purge):
    entries = list_fn()
    names = stamped(entries)
    if current not in names:
        raise RuntimeError("new backup %s not found on Drive, retention skipped" % current)
    for old in names[:-KEEP] if len(names) > KEEP else []:
        log("delete old backup %s" % old)
        purge_fn(old)
    for e in entries:
        if e["Name"].startswith(".partial-"):
            log("delete leftover %s" % e["Name"])
            purge_fn(e["Name"])
    return names[-KEEP:]


def peer_running(names, moment, max_age=None):
    max_age = PEER_MAX_AGE if max_age is None else max_age
    active = []
    for name in names:
        if not name.startswith(".partial-"):
            continue
        try:
            started = datetime.strptime(name[len(".partial-"):], "%Y-%m-%d_%H%M").astimezone()
        except ValueError:
            continue
        if (moment - started).total_seconds() < max_age:
            active.append(name)
    return sorted(active)


def list_peer():
    names = []
    for path in PEER_PATHS:
        names += [e["Name"] for e in json.loads(rclone("lsjson", "--dirs-only", path) or "[]")]
    return names


def wait_for_peer(st, problems, list_fn=list_peer, clock=now, sleep=time.sleep, tick=time.monotonic):
    if not PEER_PATHS:
        return
    t0 = tick()
    while True:
        try:
            active = peer_running(list_fn(), clock())
        except Exception as e:
            log("peer check failed, going on: %s" % str(e)[:200])
            return
        if not active:
            return
        if tick() - t0 >= PEER_WAIT:
            log("peer still running after %d s, going on" % PEER_WAIT)
            problems.append("started while the other backup was running (%s)" % active[-1])
            return
        st.set(stage="peer", peer=active[-1])
        log("waiting for the other backup: %s" % active[-1])
        sleep(PEER_POLL)


def kept_sizes(kept):
    sizes = {}
    for f in json.loads(rclone("lsjson", "-R", "--files-only", DAILY) or "[]"):
        top = f["Path"].split("/", 1)[0]
        sizes[top] = sizes.get(top, 0) + f["Size"]
    return [{"name": n, "size": sizes.get(n, 0)} for n in kept]


def lock_or_exit():
    import fcntl
    lock = open(os.path.join(STATE, "run.lock"), "w")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        print("another run is active", file=sys.stderr)
        sys.exit(3)
    return lock


def run():
    global _logf
    os.makedirs(STATE, exist_ok=True)
    lock = lock_or_exit()
    _logf = open(os.path.join(STATE, "last.log"), "w", encoding="utf-8")
    st = Status()
    previous_good = st.load().get("last_good_size")
    stamp = now().strftime("%Y-%m-%d_%H%M")
    part, final = "%s/.partial-%s" % (DAILY, stamp), "%s/%s" % (DAILY, stamp)
    t0 = time.monotonic()
    items, problems, notes = [], [], []
    st.data = {"state": "running", "stamp": stamp, "started": now().isoformat(), "stage": "preflight",
               "read_bytes": 0, "up_bytes": 0, "total_bytes": 0, "rate": 0, "eta": 0, "items": items,
               "problems": problems, "notes": notes, "path": final.split(":", 1)[1], "last_good_size": previous_good}
    st.set()
    tracker = None
    code = 0
    monitor = None
    try:
        load_token()
        wait_for_peer(st, problems)
        st.set(stage="preflight", peer=None)
        preflight(st, problems)
        rclone("mkdir", part)
        sftp = source_connect()
        try:
            st.set(stage="save")
            save_world(problems, "at start")
            st.set(stage="walk")
            entries = walk(sftp)
        finally:
            sftp_close(sftp)
        total = sum(e.size for e in entries.values())
        plan = plan_parts(entries)
        if not entries:
            raise RuntimeError("source tree is empty")
        log("walk: %d entries, %d bytes, %d parts" % (len(entries), total, len(plan)))
        tracker = Tracker(st, total)
        monitor = threading.Thread(target=tracker.monitor, daemon=True)
        monitor.start()
        st.set(stage="parts", total_bytes=total)
        with ThreadPoolExecutor(max_workers=max(STREAMS, 1)) as pool:
            futures = [pool.submit(stream_part, part, name, rels, entries, tracker) for name, rels in plan.items()]
            for f in futures:
                items.append(f.result())
                st.set(items=items)
        delta_item, deleted = run_delta(part, st, entries, tracker, problems, notes)
        items.append(delta_item)
        st.set(items=items)
        dump_databases(part, st, tracker, items, problems)
        dump_clickhouse(part, st, tracker, items, problems)
        order = [i["name"] for i in items if i["name"].endswith(".tar.zst") and not i.get("delta")] + [delta_item["name"]]
        manifest = json.dumps({"stamp": stamp, "items": items, "problems": problems, "notes": notes,
                               "excludes": EXCLUDES, "only": ONLY, "extract_order": order, "deleted": deleted},
                              ensure_ascii=False, indent=1).encode("utf-8")
        p = subprocess.run(RCLONE + ["rcat", part + "/MANIFEST.json"], input=manifest, stderr=subprocess.PIPE)
        if p.returncode:
            raise RuntimeError("manifest upload failed: %s" % p.stderr.decode("utf-8", "replace")[-300:])
        st.set(stage="finalize")
        rclone("moveto", part, final)
        st.set(stage="retention")
        kept = retention(stamp)
        st.set(kept=kept_sizes(kept), last_good_size=sum(i["size"] for i in items))
        st.set(state="ok" if not problems else "degraded")
    except Exception as e:
        log("FAILED: %s" % e)
        st.set(state="failed", error=str(e)[:500])
        code = 1
        try:
            rclone("purge", "--drive-use-trash=false", part, timeout=600)
        except Exception:
            pass
    finally:
        if tracker:
            tracker.stop.set()
    read, up = tracker.totals() if tracker else (0, 0)
    st.set(finished=now().isoformat(), seconds=round(time.monotonic() - t0), stage="", read_bytes=read, up_bytes=up,
           size=sum(i["size"] for i in items), rate=0, eta=0, problems=problems, notes=notes)
    st.archive()
    log("finished: %s in %d s" % (st.data["state"], st.data["seconds"]))
    try:
        os.remove(CONF)
    except FileNotFoundError:
        pass
    lock.close()
    return code


def mark_failed(reason):
    st = Status()
    if st.load().get("state") == "running":
        st.set(state="failed", error="aborted: %s" % reason, finished=now().isoformat(), stage="", rate=0, eta=0)
        st.archive()


def beat(path, busy):
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        f.write("1" if busy else "0")
    os.replace(tmp, path)


def serve():
    os.makedirs(STATE, exist_ok=True)
    mark_failed("worker restarted during the run")
    heart = os.path.join(STATE, "worker.heartbeat")
    request = os.path.join(STATE, "run.request")
    while True:
        beat(heart, False)
        if os.path.exists(request):
            beat(heart, True)
            os.remove(request)
            p = subprocess.Popen([sys.executable, "-m", "drivebackup.engine", "run"], stdin=subprocess.DEVNULL)
            while p.poll() is None:
                beat(heart, True)
                time.sleep(SERVE_POLL)
            log("worker: run exited %d" % p.returncode)
            beat(heart, False)
        time.sleep(SERVE_POLL)


def part_stream(stamp, name):
    cat = subprocess.Popen(RCLONE + ["cat", "%s/%s/%s" % (DAILY, stamp, name)], stdout=subprocess.PIPE)
    zst = subprocess.Popen(["zstd", "-d", "-q", "-c"], stdin=cat.stdout, stdout=subprocess.PIPE)
    cat.stdout.close()
    return cat, zst


def read_manifest(stamp):
    return json.loads(rclone("cat", "%s/%s/MANIFEST.json" % (DAILY, stamp)))


def safe_member(name):
    parts = name.split("/")
    if name.startswith("/") or ".." in parts:
        raise RuntimeError("unsafe path in archive: %s" % name)
    return name


def verify(stamp):
    load_token()
    manifest = read_manifest(stamp)
    bad = 0
    for item in manifest["items"]:
        if not item["name"].endswith(".tar.zst"):
            continue
        cat, zst = part_stream(stamp, item["name"])
        files = dirs = raw = 0
        with tarfile.open(fileobj=zst.stdout, mode="r|") as tf:
            for m in tf:
                if m.isdir():
                    dirs += 1
                elif m.isfile():
                    files += 1
                    raw += m.size
        zst.stdout.close()
        codes = (zst.wait(), cat.wait())
        ok = (files, dirs) == (item["files"], item["dirs"]) and not any(codes)
        bad += 0 if ok else 1
        print("%s %s: files %d/%d dirs %d/%d bytes %d" % (
            "ok" if ok else "BAD", item["name"], files, item["files"], dirs, item["dirs"], raw), flush=True)
    print("verify %s: %d parts bad" % (stamp, bad), flush=True)
    return 1 if bad else 0


def mkdirs(sftp, path, made):
    parent = os.path.dirname(path)
    if path in ("", "/") or path in made or parent == path:
        return
    mkdirs(sftp, parent, made)
    try:
        sftp.mkdir(path, 0o755)
    except OSError:
        sftp.stat(path)
    try:
        sftp.chmod(path, 0o755)
    except OSError:
        pass
    made.add(path)


def restore_member(sftp, tf, m, dest, made):
    target = "%s/%s" % (dest, safe_member(m.name))
    if m.isdir():
        mkdirs(sftp, target, made)
    elif m.issym():
        mkdirs(sftp, os.path.dirname(target), made)
        try:
            sftp.remove(target)
        except FileNotFoundError:
            pass
        sftp.symlink(m.linkname, target)
    elif m.isfile():
        mkdirs(sftp, os.path.dirname(target), made)
        src = tf.extractfile(m)
        with sftp.open(target, "wb") as w:
            w.set_pipelined(True)
            while True:
                chunk = src.read(BUF)
                if not chunk:
                    break
                w.write(chunk)
        written = sftp.stat(target).st_size
        if written != m.size:
            raise RuntimeError("%s: wrote %d of %d bytes" % (target, written, m.size))
        try:
            sftp.utime(target, (m.mtime, m.mtime))
        except OSError:
            pass


def restore(stamp, dest, only_part=None, live=False):
    dest = dest.rstrip("/")
    if dest == "" and not live:
        raise SystemExit("refusing to restore over / without --live")
    load_token()
    manifest = read_manifest(stamp)
    order = [only_part] if only_part else manifest["extract_order"]
    sftp = LocalFS("") if SOURCE_TYPE == "local" else sftp_connect()
    made = set()
    try:
        for name in order:
            cat, zst = part_stream(stamp, name)
            count = 0
            with tarfile.open(fileobj=zst.stdout, mode="r|") as tf:
                for m in tf:
                    restore_member(sftp, tf, m, dest, made)
                    count += 1
            zst.stdout.close()
            if zst.wait() or cat.wait():
                raise RuntimeError("%s: stream failed" % name)
            print("restored %s: %d entries" % (name, count), flush=True)
        if not only_part:
            for rel in manifest.get("deleted", []):
                try:
                    sftp.remove("%s/%s" % (dest, safe_member(rel)))
                except OSError:
                    pass
    finally:
        sftp_close(sftp)
    print("databases are not restored here: rclone cat %s/%s/mariadb/<db>.sql.zst | zstd -d | mariadb" % (DAILY, stamp))
    return 0


def main(argv):
    action = argv[1] if len(argv) > 1 else "run"
    if action == "run":
        return run()
    if action == "serve":
        return serve()
    if action == "mark-failed":
        mark_failed(" ".join(argv[2:]) or "unknown")
        return 0
    if action == "verify" and len(argv) == 3:
        return verify(argv[2])
    if action == "restore" and len(argv) >= 3:
        args = argv[3:]
        dest = part = None
        live = "--live" in args
        if "--to" in args:
            dest = args[args.index("--to") + 1]
        if "--part" in args:
            part = args[args.index("--part") + 1]
        if dest is None:
            raise SystemExit("restore needs --to <path>")
        return restore(argv[2], dest, part, live)
    print("usage: drivebackup.engine run | serve | mark-failed [reason] | verify <stamp> | "
          "restore <stamp> --to <path> [--part <file>] [--live]", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv))
