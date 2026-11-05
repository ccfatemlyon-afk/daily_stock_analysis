# -*- coding: utf-8 -*-
"""Guard: litellm streaming success logging stays disabled (native crash fix).

Why (2026-11-06, two production runs, identical faulthandler stacks)
--------------------------------------------------------------------
Every streamed response is finished by litellm on a **thread-pool worker**
running ``asyncio.run(async_success_handler(...))``
(``litellm/litellm_core_utils/streaming_handler.py:1771``; the branch is taken
because this project calls ``litellm.completion()`` without a ``logging_loop``).
On Windows, building an event loop in that thread dies in the loop's self-pipe
(``socket.socketpair()``) with ``0xC0000005`` — a native crash Python cannot
catch, so the whole process disappears (no report, no daily mail).

P1-15 switched the event-loop policy to Selector and removed the *proactor*
frames, but the crash point survived; both DSA runs still died at ~636s. The
switch below removes the code path entirely.

Cost: none for accounting — this repo has **no** litellm success callback /
CustomLogger usage (``src/`` has 0 references); token usage is read from the
response object by DSA itself (``src/analyzer.py::_normalize_usage`` →
``persist_llm_usage``). The flag only short-circuits litellm's own async logging
and internal cache.

These tests are static-first on purpose: importing ``main`` boots config/env.
"""
from __future__ import annotations

import ast
import types
from pathlib import Path

import pytest

MAIN_PATH = Path(__file__).resolve().parents[1] / "main.py"
FUNC_NAME = "_disable_litellm_streaming_logging"


def _source() -> str:
    return MAIN_PATH.read_text(encoding="utf-8")


def _function_node() -> ast.FunctionDef:
    for node in ast.parse(_source()).body:
        if isinstance(node, ast.FunctionDef) and node.name == FUNC_NAME:
            return node
    raise AssertionError(f"main.py 缺少 {FUNC_NAME}()")


def _main_block() -> list[ast.stmt]:
    for node in ast.parse(_source()).body:
        if isinstance(node, ast.If) and "__main__" in ast.dump(node.test):
            return list(node.body)
    raise AssertionError("main.py 缺少 __main__ 块")


def _called_names(stmts: list[ast.stmt]) -> list[str]:
    names: list[str] = []
    for stmt in stmts:
        for node in ast.walk(stmt):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                names.append(node.func.id)
    return names


class TestStaticContract:
    def test_function_exists(self) -> None:
        assert _function_node().name == FUNC_NAME

    def test_called_at_entry_point(self) -> None:
        """必须在 `__main__` 里被调用（否则只有 interactive/WebUI 路径生效不了）。"""
        assert FUNC_NAME in _called_names(_main_block())

    def test_called_after_policy_and_before_main(self) -> None:
        """顺序契约：策略切换 → 关日志 → `main()`。

        策略在前是 P1-15 的既有契约；关日志必须在 `main()` 之前，否则第一轮
        流式响应收尾就跑在危险路径上。
        """
        order = _called_names(_main_block())
        assert "_apply_windows_event_loop_policy" in order
        assert order.index("_apply_windows_event_loop_policy") < order.index(FUNC_NAME)
        assert order.index(FUNC_NAME) < order.index("main")

    def test_litellm_imported_lazily_inside_function(self) -> None:
        """不能在模块顶层 import litellm：main.py 要被 WebUI/测试轻量导入。"""
        imports = [
            node for node in ast.walk(_function_node())
            if isinstance(node, ast.Import)
            and any(alias.name == "litellm" for alias in node.names)
        ]
        assert imports, "函数体内应自行 import litellm（延迟导入）"


class TestBehaviour:
    def test_sets_flag_true(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """执行函数源码，确认把 `litellm.disable_streaming_logging` 置为 True。"""
        fake = types.ModuleType("litellm")
        fake.disable_streaming_logging = False  # type: ignore[attr-defined]
        monkeypatch.setitem(__import__("sys").modules, "litellm", fake)

        source = ast.get_source_segment(_source(), _function_node())
        namespace: dict = {"logger": types.SimpleNamespace(warning=lambda *a, **k: None)}
        exec(compile(source, str(MAIN_PATH), "exec"), namespace)  # noqa: S102
        namespace[FUNC_NAME]()

        assert fake.disable_streaming_logging is True  # type: ignore[attr-defined]

    def test_failure_does_not_raise(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """litellm 不可用时只告警，不得让主流程起不来。"""
        import sys as _sys

        warnings: list[str] = []
        monkeypatch.setitem(_sys.modules, "litellm", None)  # import 直接失败

        source = ast.get_source_segment(_source(), _function_node())
        namespace: dict = {
            "logger": types.SimpleNamespace(warning=lambda fmt, *a, **k: warnings.append(fmt))
        }
        exec(compile(source, str(MAIN_PATH), "exec"), namespace)  # noqa: S102
        namespace[FUNC_NAME]()

        assert warnings, "失败路径必须留下告警日志"
