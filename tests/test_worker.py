import asyncio
import os
import sys
import time

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from drivebackup import engine as nb
from drivebackup import panel


class Stop(Exception):
    pass


def write(path, text):
    with open(path, "w") as f:
        f.write(text)


def test_start_refused_when_worker_is_dead(tmp_path):
    w = panel.Worker(str(tmp_path))
    assert asyncio.run(w.start()) == "down"
    assert not os.path.exists(w.request)


def test_start_writes_request_then_reports_busy(tmp_path):
    w = panel.Worker(str(tmp_path))
    write(w.heart, "0")
    assert asyncio.run(w.start()) == "started"
    assert os.path.exists(w.request) and w.running()
    assert asyncio.run(w.start()) == "busy"


def test_stale_heartbeat_means_dead_and_not_running(tmp_path):
    w = panel.Worker(str(tmp_path))
    write(w.heart, "1")
    old = time.time() - panel.WORKER_STALE - 10
    os.utime(w.heart, (old, old))
    assert not w.alive() and not w.running()


def test_serve_runs_engine_module_once(tmp_path, monkeypatch):
    monkeypatch.setattr(nb, "STATE", str(tmp_path))
    monkeypatch.setattr(nb.time, "sleep", lambda _: (_ for _ in ()).throw(Stop()))
    runs = []

    class Done:
        returncode = 0

        def __init__(self, cmd, **kw):
            runs.append(cmd)

        def poll(self):
            return 0

    monkeypatch.setattr(nb.subprocess, "Popen", Done)
    write(os.path.join(str(tmp_path), "run.request"), "1")
    with pytest.raises(Stop):
        nb.serve()
    assert runs[0][1:] == ["-m", "drivebackup.engine", "run"]
    assert not os.path.exists(os.path.join(str(tmp_path), "run.request"))
