import io
import os
import stat as statmod
import sys
import tarfile
import time
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from drivebackup import engine as nb


class FakeFile:
    def __init__(self, data, on_read=None):
        self.buf = io.BytesIO(data)
        self.on_read = on_read

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def prefetch(self, size, requests=None):
        pass

    def read(self, n=-1):
        if self.on_read:
            self.on_read()
            self.on_read = None
        return self.buf.read(n)


class FakeSftp:
    def __init__(self, files=None, dirs=()):
        self.files = dict(files or {})
        self.dirs = set(dirs)
        self.on_open = {}

    def stat(self, path):
        rel = path.lstrip("/")
        if rel not in self.files:
            raise FileNotFoundError(path)
        data, mtime = self.files[rel]
        return SimpleNamespace(st_size=len(data), st_mtime=mtime, st_mode=statmod.S_IFREG | 0o644)

    def open(self, path, mode="rb"):
        rel = path.lstrip("/")
        if rel not in self.files:
            raise FileNotFoundError(path)
        return FakeFile(self.files[rel][0], self.on_open.pop(rel, None))

    def listdir_attr(self, path):
        folder = path.strip("/")
        prefix = folder + "/" if folder else ""
        seen, out = set(), []
        for rel, (data, mtime) in self.files.items():
            if rel.startswith(prefix):
                head = rel[len(prefix):].split("/")[0]
                if head in seen:
                    continue
                seen.add(head)
                if "/" in rel[len(prefix):]:
                    out.append(SimpleNamespace(filename=head, st_mode=statmod.S_IFDIR | 0o755, st_size=0, st_mtime=mtime))
                else:
                    out.append(SimpleNamespace(filename=head, st_mode=statmod.S_IFREG | 0o644, st_size=len(data), st_mtime=mtime))
        return out


def tracker():
    st = SimpleNamespace(set=lambda **kw: None)
    return nb.Tracker(st, 0)


def build_tar(sftp, rels, entries, tr):
    out = io.BytesIO()
    attempt = nb.Attempt("t")
    files, dirs = nb.write_tar(out, sftp, rels, entries, tr, attempt)
    out.seek(0)
    with tarfile.open(fileobj=out, mode="r|") as tf:
        got = {}
        for m in tf:
            got[m.name] = (m, tf.extractfile(m).read() if m.isfile() else None)
    return got, files, dirs, attempt


def test_walk_applies_excludes():
    sftp = FakeSftp({"world/level.dat": (b"a", 1), "logs/latest.log": (b"b", 1), "plugins/.paper-remapped/x": (b"c", 1),
                     "plugins/a.jar": (b"d", 1), "world/session.lock": (b"e", 1), "old/z": (b"f", 1)})
    entries = nb.walk(sftp, excludes=nb.EXCLUDES, only=())
    assert "world/level.dat" in entries and "plugins/a.jar" in entries
    assert "old/z" in entries
    assert not [r for r in entries if r.startswith(("logs", "plugins/.paper"))]
    assert "world/session.lock" not in entries


def test_walk_only_limits_tree():
    sftp = FakeSftp({"config/a/b.toml": (b"a", 1), "world/level.dat": (b"b", 1)})
    entries = nb.walk(sftp, excludes=(), only=("config/a",))
    assert set(entries) == {"config", "config/a", "config/a/b.toml"}


def test_part_mapping():
    assert nb.part_of("mods/x.jar") == "server-files"
    assert nb.part_of("world/DIM-1/region/r.0.0.mca") == "world-nether"
    assert nb.part_of("world/DIM1/region/r.0.0.mca") == "world-end"
    assert nb.part_of("world/dimensions/ad_astra/mars/region/r.0.0.mca") == "world-ad_astra-mars"
    assert nb.part_of("world/region/r.0.0.mca") == "world-overworld"
    assert nb.part_of("world/playerdata/a.dat") == "world-overworld"
    assert list(nb.plan_parts({"world/a": 0, "mods/b": 0}))[0] == "server-files"


def test_small_file_roundtrip():
    sftp = FakeSftp({"a/b.txt": (b"hello", 1000)})
    entries = {"a": nb.Entry("d", 0, 1000, 0o755), "a/b.txt": nb.Entry("f", 5, 1000, 0o644)}
    tr = tracker()
    got, files, dirs, attempt = build_tar(sftp, ["a", "a/b.txt"], entries, tr)
    assert got["a/b.txt"][1] == b"hello"
    assert got["a"][0].isdir()
    assert (files, dirs) == (1, 1)
    assert attempt.read == 5
    assert tr.captured["a/b.txt"] == (5, 1000)
    assert "a/b.txt" not in tr.dirty


def test_changed_during_read_is_reread():
    sftp = FakeSftp({"f": (b"old", 1000)})

    def mutate():
        sftp.files["f"] = (b"newer", 1001)
    sftp.on_open["f"] = mutate
    tr = tracker()
    got, *_ = build_tar(sftp, ["f"], {"f": nb.Entry("f", 3, 1000, 0o644)}, tr)
    assert got["f"][1] == b"newer"
    assert tr.captured["f"] == (5, 1001)
    assert "f" not in tr.dirty


def test_always_changing_file_goes_dirty():
    class Churn(FakeSftp):
        n = 1000

        def stat(self, path):
            Churn.n += 1
            return SimpleNamespace(st_size=3, st_mtime=Churn.n, st_mode=statmod.S_IFREG | 0o644)
    sftp = Churn({"f": (b"abc", 1000)})
    tr = tracker()
    got, *_ = build_tar(sftp, ["f"], {"f": nb.Entry("f", 3, 1000, 0o644)}, tr)
    assert got["f"][1] == b"abc"
    assert "f" in tr.dirty


def test_recent_mtime_is_racy():
    now = int(time.time())
    sftp = FakeSftp({"f": (b"abc", now)})
    tr = tracker()
    build_tar(sftp, ["f"], {"f": nb.Entry("f", 3, now, 0o644)}, tr)
    assert "f" in tr.dirty


def test_vanished_file_is_skipped():
    tr = tracker()
    got, files, *_ = build_tar(FakeSftp(), ["gone"], {"gone": nb.Entry("f", 3, 1, 0o644)}, tr)
    assert got == {} and files == 0
    assert "gone" in tr.vanished


def test_big_file_shrunk_is_padded_and_dirty(monkeypatch):
    monkeypatch.setattr(nb, "SMALL_LIMIT", 4)

    class Shrinking(FakeSftp):
        def open(self, path, mode="rb"):
            return FakeFile(b"01234")
    sftp = Shrinking({"big": (b"0123456789", 1000)})
    tr = tracker()
    got, *_ = build_tar(sftp, ["big"], {"big": nb.Entry("f", 10, 1000, 0o644)}, tr)
    assert got["big"][1] == b"01234" + b"\0" * 5
    assert "big" in tr.dirty


def test_big_file_grown_keeps_header_size_and_dirty(monkeypatch):
    monkeypatch.setattr(nb, "SMALL_LIMIT", 4)

    class Growing(FakeSftp):
        def open(self, path, mode="rb"):
            self.files["big"] = (b"0123456789ABCDEF", 1001)
            return FakeFile(b"0123456789ABCDEF")
    sftp = Growing({"big": (b"0123456789", 1000)})
    tr = tracker()
    got, *_ = build_tar(sftp, ["big"], {"big": nb.Entry("f", 10, 1000, 0o644)}, tr)
    assert got["big"][1] == b"0123456789"
    assert "big" in tr.dirty


def test_parallel_readers_keep_order_and_window(monkeypatch):
    monkeypatch.setattr(nb, "WINDOW_FILES", 3)
    files = {"d/f%02d" % i: (b"x" * (i + 1), 1000) for i in range(30)}
    files.pop("d/f07")
    sftps = [FakeSftp(files) for _ in range(3)]
    entries = {"d": nb.Entry("d", 0, 1000, 0o755)}
    for i in range(30):
        entries["d/f%02d" % i] = nb.Entry("f", i + 1, 1000, 0o644)
    rels = sorted(entries)
    tr = tracker()
    out = io.BytesIO()
    attempt = nb.Attempt("t")
    filecount, dirs = nb.write_tar(out, sftps[0], rels, entries, tr, attempt, sftps)
    out.seek(0)
    with tarfile.open(fileobj=out, mode="r|") as tf:
        members = [(m.name, tf.extractfile(m).read() if m.isfile() else None) for m in tf]
    assert members[0][0] == "d"
    assert [n for n, _ in members[1:]] == [r for r in rels[1:] if r != "d/f07"]
    assert all(data == b"x" * (int(n[-2:]) + 1) for n, data in members[1:])
    assert (filecount, dirs) == (29, 1)
    assert tr.vanished == {"d/f07"}
    assert attempt.read == sum(i + 1 for i in range(30) if i != 7)


def test_reader_error_fails_the_part():
    class Broken(FakeSftp):
        def open(self, path, mode="rb"):
            raise OSError("connection lost")
    sftp = Broken({"f": (b"abc", 1000)})
    with pytest.raises(OSError):
        nb.write_tar(io.BytesIO(), sftp, ["f"], {"f": nb.Entry("f", 3, 1000, 0o644)}, tracker(), nb.Attempt("t"))


def test_delta_selection():
    captured = {"same": (3, 10), "edited": (3, 10), "dirty": (3, 10)}
    second = {"same": nb.Entry("f", 3, 10, 0o644), "edited": nb.Entry("f", 3, 11, 0o644),
              "dirty": nb.Entry("f", 3, 10, 0o644), "new": nb.Entry("f", 1, 10, 0o644),
              "newdir": nb.Entry("d", 0, 10, 0o755), "newlink": nb.Entry("l", 0, 10, 0o777)}
    assert nb.changed_files(captured, {"dirty"}, second) == ["dirty", "edited", "new"]
    first = {"same": 0, "gone": 0}
    assert nb.deleted_paths(first, second) == ["gone"]
    assert nb.new_nonfiles(first, second) == ["newdir", "newlink"]


def names(*stamps):
    return [{"Name": s} for s in stamps]


STAMPS = ["2026-11-0%d_0000" % d for d in range(1, 7)]


def test_retention_keeps_newest_five(monkeypatch):
    monkeypatch.setattr(nb, "KEEP", 5)
    removed = []
    kept = nb.retention(STAMPS[-1], list_fn=lambda: names(*STAMPS, ".partial-x"), purge_fn=removed.append)
    assert removed == [STAMPS[0], ".partial-x"]
    assert kept == STAMPS[1:]


def test_retention_refuses_without_new_copy(monkeypatch):
    monkeypatch.setattr(nb, "KEEP", 5)
    removed = []
    with pytest.raises(RuntimeError):
        nb.retention("2026-12-01_0000", list_fn=lambda: names(*STAMPS), purge_fn=removed.append)
    assert removed == []


def test_retention_ignores_foreign_folders(monkeypatch):
    monkeypatch.setattr(nb, "KEEP", 1)
    removed = []
    nb.retention(STAMPS[1], list_fn=lambda: names(STAMPS[0], STAMPS[1], "keep-me"), purge_fn=removed.append)
    assert removed == [STAMPS[0]]


def preflight_env(tmp_path, monkeypatch):
    monkeypatch.setattr(nb, "STATE", str(tmp_path))
    monkeypatch.setattr(nb, "KEEP", 5)
    monkeypatch.setattr(nb, "SIZE_ESTIMATE", 100)


def test_preflight_enough_room(tmp_path, monkeypatch):
    preflight_env(tmp_path, monkeypatch)
    removed, problems = [], []
    nb.preflight(None, problems, list_fn=lambda: names(*STAMPS[:5]), free_fn=lambda: 1000, purge_fn=removed.append)
    assert removed == [] and problems == []


def test_preflight_deletes_oldest_when_full(tmp_path, monkeypatch):
    preflight_env(tmp_path, monkeypatch)
    removed, problems = [], []
    free = iter([50, 500])
    nb.preflight(None, problems, list_fn=lambda: names(*STAMPS[:5]), free_fn=lambda: next(free), purge_fn=removed.append)
    assert removed == [STAMPS[0]]
    assert len(problems) == 1


def test_preflight_fails_when_copies_too_few(tmp_path, monkeypatch):
    preflight_env(tmp_path, monkeypatch)
    removed = []
    with pytest.raises(RuntimeError):
        nb.preflight(None, [], list_fn=lambda: names(*STAMPS[:2]), free_fn=lambda: 50, purge_fn=removed.append)
    assert removed == []


def test_preflight_fails_when_still_short(tmp_path, monkeypatch):
    preflight_env(tmp_path, monkeypatch)
    removed = []
    with pytest.raises(RuntimeError):
        nb.preflight(None, [], list_fn=lambda: names(*STAMPS[:5]), free_fn=lambda: 50, purge_fn=removed.append)
    assert removed == [STAMPS[0]]


def test_gdrive_section_only():
    text = "[joinserver]\npass = secret\n\n[gdrive]\ntype = drive\ntoken = t\n\n[other]\nx = y\n"
    out = nb.gdrive_section(text)
    assert "joinserver" not in out and "secret" not in out and "other" not in out
    assert "type = drive" in out
    with pytest.raises(RuntimeError):
        nb.gdrive_section("[joinserver]\n")


def test_unsafe_archive_paths_refused():
    assert nb.safe_member("a/b") == "a/b"
    for bad in ("/etc/passwd", "../x", "a/../../x"):
        with pytest.raises(RuntimeError):
            nb.safe_member(bad)


def test_bandwidth_split(monkeypatch):
    monkeypatch.setattr(nb, "BWLIMIT_MB", 10.0)
    monkeypatch.setattr(nb, "STREAMS", 2)
    assert nb.bw_arg() == "4.77M"


def test_restore_refuses_root_without_live():
    with pytest.raises(SystemExit):
        nb.restore("x", "/")


MOMENT = nb.datetime(2026, 11, 7, 7, 15).astimezone()


class StatusStub:
    def __init__(self):
        self.calls = []

    def set(self, **kw):
        self.calls.append(kw)


def clock_ticks(step=60):
    state = {"t": 0}

    def tick():
        state["t"] += step
        return state["t"]

    return tick


def test_peer_running_ignores_finished_and_stale():
    names = ["2026-11-06_0230", ".partial-2026-11-07_0230", ".partial-2026-11-05_0230", ".partial-bad"]
    assert nb.peer_running(names, MOMENT, max_age=21 * 3600) == [".partial-2026-11-07_0230"]


def test_gate_returns_when_peer_finishes(monkeypatch):
    monkeypatch.setattr(nb, "PEER_PATHS", ("gdrive:x",))
    answers = [[".partial-2026-11-07_0700"], [".partial-2026-11-07_0700"], ["2026-11-07_0700"]]
    naps, problems, st = [], [], StatusStub()
    nb.wait_for_peer(st, problems, list_fn=lambda: answers.pop(0), clock=lambda: MOMENT,
                     sleep=naps.append, tick=clock_ticks())
    assert len(naps) == 2 and problems == [] and st.calls[0]["stage"] == "peer"


def test_gate_gives_up_after_limit_and_reports(monkeypatch):
    monkeypatch.setattr(nb, "PEER_PATHS", ("gdrive:x",))
    monkeypatch.setattr(nb, "PEER_WAIT", 120)
    problems = []
    nb.wait_for_peer(StatusStub(), problems, list_fn=lambda: [".partial-2026-11-07_0700"], clock=lambda: MOMENT,
                     sleep=lambda _: None, tick=clock_ticks())
    assert len(problems) == 1 and "other backup" in problems[0]


def test_gate_survives_listing_error(monkeypatch):
    monkeypatch.setattr(nb, "PEER_PATHS", ("gdrive:x",))

    def boom():
        raise RuntimeError("directory not found")

    problems = []
    nb.wait_for_peer(StatusStub(), problems, list_fn=boom, clock=lambda: MOMENT, sleep=lambda _: None)
    assert problems == []


def test_gate_off_without_peer(monkeypatch):
    monkeypatch.setattr(nb, "PEER_PATHS", ())
    nb.wait_for_peer(StatusStub(), [], list_fn=lambda: pytest.fail("must not list"))


def make_tree(root):
    files = {"server.properties": b"motd=x", "world/level.dat": b"level", "world/region/r.0.0.mca": b"region",
             "world_nether/region/r.0.0.mca": b"nether", "logs/latest.log": b"skip"}
    for rel, data in files.items():
        p = root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)


def test_local_source_tar_and_restore_roundtrip(tmp_path, monkeypatch):
    src, dst = tmp_path / "src", tmp_path / "dst"
    make_tree(src)
    fs = nb.LocalFS(str(src))
    entries = nb.walk(fs, excludes=("logs",), only=())
    assert "logs/latest.log" not in entries
    plan = nb.plan_parts(entries)
    assert set(plan) == {"server-files", "world-overworld", "world-nether"}
    tr = nb.Tracker(SimpleNamespace(set=lambda **k: None), 0)
    rels = sorted(entries)
    out = io.BytesIO()
    nb.write_tar(out, fs, rels, entries, tr, nb.Attempt("all"), [nb.LocalFS(str(src))])
    out.seek(0)
    target = nb.LocalFS("")
    dst.mkdir()
    made = set()
    with tarfile.open(fileobj=out) as tf:
        for m in tf:
            nb.restore_member(target, tf, m, str(dst).replace(os.sep, "/"), made)
    assert (dst / "world" / "region" / "r.0.0.mca").read_bytes() == b"region"
    assert (dst / "server.properties").read_bytes() == b"motd=x"


def test_source_connect_rejects_unknown_type(monkeypatch):
    monkeypatch.setattr(nb, "SOURCE_TYPE", "ftp")
    with pytest.raises(RuntimeError):
        nb.source_connect()


def test_save_world_off_without_rcon(monkeypatch):
    monkeypatch.setattr(nb, "RCON_HOST", "")
    problems = []
    nb.save_world(problems, "at start")
    assert problems == []


def test_save_world_failure_is_reported_not_fatal(monkeypatch):
    monkeypatch.setattr(nb, "RCON_HOST", "127.0.0.1")
    monkeypatch.setattr(nb, "RCON_PORT", 1)
    problems = []
    nb.save_world(problems, "at start")
    assert len(problems) == 1


def test_db_args_tls_modes(monkeypatch):
    monkeypatch.setattr(nb, "MARIADB_HOST", "db")
    monkeypatch.setattr(nb, "MARIADB_USER", "u")
    monkeypatch.setattr(nb, "MARIADB_SSL_CA", "/ca.pem")
    assert "--ssl-ca=/ca.pem" in nb.db_args()
    monkeypatch.setattr(nb, "MARIADB_SSL_CA", "")
    monkeypatch.setattr(nb, "MARIADB_SSL", "on")
    assert "--skip-ssl-verify-server-cert" in nb.db_args()
    monkeypatch.setattr(nb, "MARIADB_SSL", "off")
    assert "--skip-ssl" in nb.db_args()


def test_db_names_prefers_explicit_list(monkeypatch):
    monkeypatch.setattr(nb, "MARIADB_DATABASES", ("a", "b"))
    assert nb.db_names() == ["a", "b"]


def test_clickhouse_views_are_schema_only():
    tables, views = nb.split_tables([["t1", "MergeTree"], ["v1", "View"], ["t2", "CoalescingMergeTree"]])
    assert tables == ["t1", "t2"] and views == ["v1"]


def test_retrying_recovers_once(monkeypatch):
    monkeypatch.setattr(nb, "DB_ATTEMPTS", 2)
    calls = []

    def flaky():
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError("IncompleteRead")
        return "ok"

    assert nb.retrying("x", flaky, sleep=lambda _: None) == "ok"
