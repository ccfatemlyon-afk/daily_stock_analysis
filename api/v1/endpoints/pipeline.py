# -*- coding: utf-8 -*-
"""Pipeline trigger endpoint.

Runs the stock-pipeline orchestrator (``bridge.py``) as a background
subprocess so the Web UI can launch the full AlphaEvo -> AlphaSift -> DSA
pipeline and stream its logs. ``bridge.py`` lives one level above this
repo's ``vendor/`` directory (the stock-pipeline root).
"""
from __future__ import annotations

import os
import subprocess
import threading
import uuid
from pathlib import Path

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter()

_DSA_DIR = Path(__file__).resolve().parents[3]  # .../daily_stock_analysis

_PIPELINE_TIMEOUT_SEC = int(os.getenv("DSA_PIPELINE_TIMEOUT_SEC", "3600"))
_MAX_LOG_CHARS = int(os.getenv("DSA_PIPELINE_MAX_LOG_CHARS", "1048576"))  # 1 MB


def _resolve_root() -> Path:
    # bridge.py and venvs/ live in the stock-pipeline root (one level above vendor/)
    for candidate in (_DSA_DIR.parent.parent, _DSA_DIR.parent):
        if (candidate / "bridge.py").is_file():
            return candidate
    return _DSA_DIR.parent.parent


_ROOT = _resolve_root()
BRIDGE_PY = _ROOT / "bridge.py"
PYTHON_EXE = _ROOT / "venvs" / "orch" / "Scripts" / "python.exe"

# ponytail: in-process task store; swap to file/Redis if multi-worker is used
_TASKS: dict[str, dict] = {}
_LOCK = threading.Lock()


class RunResponse(BaseModel):
    run_id: str


class TaskResponse(BaseModel):
    run_id: str
    status: str
    log: str
    exit_code: int | None


def _run_bridge(run_id: str) -> None:
    try:
        if not BRIDGE_PY.is_file():
            raise FileNotFoundError(f"bridge.py not found at {BRIDGE_PY}")
        if not PYTHON_EXE.is_file():
            raise FileNotFoundError(f"orch python not found at {PYTHON_EXE}")
        # ponytail: force UTF-8 mode so bridge.py's own file reads stop hitting GBK
        env = os.environ.copy()
        env.setdefault("PYTHONIOENCODING", "utf-8")
        env.setdefault("PYTHONUTF8", "1")
        proc = subprocess.Popen(
            [str(PYTHON_EXE), str(BRIDGE_PY)],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            cwd=str(_ROOT),
            env=env,
        )
        buf: list[str] = []
        total_chars = 0
        assert proc.stdout is not None
        for raw in iter(proc.stdout.readline, b""):
            line = raw.decode("utf-8", errors="replace")
            total_chars += len(line)
            if total_chars <= _MAX_LOG_CHARS:
                buf.append(line)
            with _LOCK:
                _TASKS[run_id]["log"] = "".join(buf)
        proc.stdout.close()
        try:
            proc.wait(timeout=_PIPELINE_TIMEOUT_SEC)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
            with _LOCK:
                _TASKS[run_id]["status"] = "failed"
                _TASKS[run_id]["log"] += f"\n[error] Pipeline timed out after {_PIPELINE_TIMEOUT_SEC}s"
                _TASKS[run_id]["exit_code"] = -1
            return
        with _LOCK:
            _TASKS[run_id]["status"] = "completed" if proc.returncode == 0 else "failed"
            _TASKS[run_id]["exit_code"] = proc.returncode
    except Exception as exc:  # noqa: BLE001
        with _LOCK:
            _TASKS[run_id]["status"] = "failed"
            _TASKS[run_id]["log"] += f"\n[error] {exc}"
            _TASKS[run_id]["exit_code"] = -1


@router.post("/run", response_model=RunResponse)
async def run_pipeline() -> RunResponse:
    run_id = uuid.uuid4().hex
    with _LOCK:
        _TASKS[run_id] = {"status": "running", "log": "", "exit_code": None}
    threading.Thread(target=_run_bridge, args=(run_id,), daemon=True).start()
    return RunResponse(run_id=run_id)


@router.get("/tasks/{run_id}", response_model=TaskResponse)
async def get_task(run_id: str) -> TaskResponse:
    with _LOCK:
        task = _TASKS.get(run_id)
    if task is None:
        raise HTTPException(status_code=404, detail="run not found")
    return TaskResponse(run_id=run_id, **task)
