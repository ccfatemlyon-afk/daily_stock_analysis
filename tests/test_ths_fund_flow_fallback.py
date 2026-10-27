# -*- coding: utf-8 -*-
"""S-7：个股资金流兜底（同花顺预热缓存）的守卫测试。

病根：`get_capital_flow` 只走东财 push2his 一脉（`stock_individual_fund_flow` 等），
该族自 2026-09-15 起间歇 `RemoteDisconnected`；DB 逐日实证 `stock_flow ok=0`
⇒ analyzer 的 fail-soft 门把**所有**买入结论降级「持有观察」+ 评分钳 [45,59]。

兜底形态：编排层每日把同花顺**全市场**表预热成缓存文件（66s / 105 页，塞不进
DSA 的 8s 逐股预算），适配器只读文件。这里钉住三件事：

1. 缓存缺失 / 过期 / 坏文件 / 无该股 ⇒ 一律**不抛**，且 reason 可辨；
2. 命中时字段与东财路径同位（`main_net_inflow` / `inflow_5d` / `inflow_10d`），
   而 5/10 日**显式 None**（同花顺「即时」表本来就没有，不拿别的窗口顶替）；
3. 兜底拿到的数**不冒充 ok**：`stock_flow_source` / `stock_flow_degraded` /
   `stock_flow_metric` 必须透到 capital_flow 块，块状态是 `degraded`。
"""
import json
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data_provider.base import DataFetcherManager
from data_provider.fundamental_adapter import (
    AkshareFundamentalAdapter,
    load_ths_fund_flow,
)

_CACHE = {
    "source": "ths:stock_fund_flow_individual",
    "metric": "net_inflow_all_orders",
    "degraded": True,
    "flow": {
        "601398": {
            "main_net_inflow": 618000000.0,
            "inflow_today": 2373000000.0,
            "outflow_today": 1755000000.0,
            "inflow_5d": None,
            "inflow_10d": None,
            "raw": "6.18亿",
        }
    },
}


class _AkshareBlind:
    """东财全挂的 akshare 桩：任何候选函数都抛，模拟 RemoteDisconnected。"""

    def __getattr__(self, name):
        def _boom(**kwargs):
            raise ConnectionError("RemoteDisconnected: push2his")
        return _boom


class TestThsCacheReader(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / "ths_fund_flow_latest.json"
        self.path.write_text(json.dumps(_CACHE, ensure_ascii=False), encoding="utf-8")
        self.patcher = patch.dict(os.environ, {"THS_FUND_FLOW_CACHE_FILE": str(self.path)})
        self.patcher.start()
        self.addCleanup(self.patcher.stop)

    def test_missing_cache_is_none_with_reason(self) -> None:
        self.path.unlink()
        flow, reason = load_ths_fund_flow("601398")
        self.assertIsNone(flow)
        self.assertTrue(reason.startswith("ths_cache_missing"), reason)

    def test_hit_keeps_field_names_and_leaves_5d_10d_empty(self) -> None:
        flow, reason = load_ths_fund_flow("601398.SH")
        self.assertIsNone(reason)
        self.assertEqual(flow["main_net_inflow"], 618000000.0)
        self.assertIsNone(flow["inflow_5d"], "同花顺即时表没有 5 日值，不能拿别的窗口顶替")
        self.assertIsNone(flow["inflow_10d"])

    def test_stale_cache_is_rejected(self) -> None:
        old = time.time() - 5 * 24 * 3600
        os.utime(self.path, (old, old))
        flow, reason = load_ths_fund_flow("601398")
        self.assertIsNone(flow)
        self.assertTrue(reason.startswith("ths_cache_stale"), reason)

    def test_corrupt_cache_does_not_raise(self) -> None:
        self.path.write_text("{not json", encoding="utf-8")
        flow, reason = load_ths_fund_flow("601398")
        self.assertIsNone(flow)
        self.assertTrue(reason.startswith("ths_cache_invalid"), reason)

    def test_unknown_symbol_is_reported_not_zero(self) -> None:
        flow, reason = load_ths_fund_flow("000001")
        self.assertIsNone(flow, "缓存里没有 ≠ 净额为 0")
        self.assertEqual(reason, "ths_cache_no_symbol")

    def test_code_normalization_strips_suffix_and_pads(self) -> None:
        cache = dict(_CACHE)
        cache["flow"] = {"000001": {"main_net_inflow": 123.0, "inflow_5d": None,
                                    "inflow_10d": None}}
        self.path.write_text(json.dumps(cache), encoding="utf-8")
        flow, reason = load_ths_fund_flow("1")  # 深市代码在同花顺表里是整数 1
        self.assertIsNone(reason)
        self.assertEqual(flow["main_net_inflow"], 123.0)


class TestAdapterFallback(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / "ths_fund_flow_latest.json"
        self.path.write_text(json.dumps(_CACHE, ensure_ascii=False), encoding="utf-8")
        for patcher in (
            patch.dict(os.environ, {"THS_FUND_FLOW_CACHE_FILE": str(self.path)}),
            patch.dict(sys.modules, {"akshare": _AkshareBlind()}),
        ):
            patcher.start()
            self.addCleanup(patcher.stop)

    def test_eastmoney_dead_uses_cache_and_marks_degraded(self) -> None:
        payload = AkshareFundamentalAdapter().get_capital_flow("601398")
        self.assertEqual(payload["stock_flow"]["main_net_inflow"], 618000000.0)
        self.assertEqual(payload["stock_flow_source"], "ths")
        self.assertTrue(payload["stock_flow_degraded"])
        self.assertEqual(payload["stock_flow_metric"], "net_inflow_all_orders")
        self.assertIn("capital_stock:ths_cache", payload["source_chain"])
        self.assertIn("eastmoney_unavailable:ths_cache_used", payload["errors"])
        # 有内容 ⇒ partial（不是 not_supported；也不是 ok）
        self.assertEqual(payload["status"], "partial")

    def test_no_cache_keeps_old_behaviour_and_says_why(self) -> None:
        self.path.unlink()
        payload = AkshareFundamentalAdapter().get_capital_flow("601398")
        self.assertEqual(payload["stock_flow"], {})
        self.assertEqual(payload["status"], "not_supported")
        self.assertNotIn("capital_stock:ths_cache", payload["source_chain"])
        self.assertTrue(any("ths_cache_missing" in e for e in payload["errors"]))


class _AkshareHanging:
    """东财不报错、就是挂着（akshare 的 requests 没超时，实测单次能挂 60s）。"""

    def __getattr__(self, name):
        def _hang(**kwargs):
            time.sleep(30)
            raise AssertionError("不该等到这里")
        return _hang


class TestBoundedEastmoney(unittest.TestCase):
    """没有硬预算 ⇒ 外层 8s 把整个函数放弃 ⇒ 兜底分支是死代码。"""

    def test_hanging_candidate_is_abandoned_within_budget(self) -> None:
        from data_provider import fundamental_adapter as fa

        adapter = AkshareFundamentalAdapter()
        with patch.dict(sys.modules, {"akshare": _AkshareHanging()}), \
                patch.object(fa, "CAPITAL_FLOW_EM_BUDGET_SEC", 0.3):
            started = time.monotonic()
            df, source, errors = adapter._call_df_candidates(
                [("stock_individual_fund_flow", {"stock": "601398"})],
                budget_seconds=0.3)
            elapsed = time.monotonic() - started

        self.assertIsNone(df)
        self.assertIsNone(source)
        self.assertLess(elapsed, 5.0, f"硬预算没生效，耗了 {elapsed:.1f}s")
        self.assertTrue(any("timeout" in e for e in errors), errors)

    def test_without_budget_behaviour_is_unchanged(self) -> None:
        class _Fast:
            def stock_individual_fund_flow(self, **_kwargs):
                import pandas as pd
                return pd.DataFrame([{"股票代码": "601398", "值": 1}])

        adapter = AkshareFundamentalAdapter()
        with patch.dict(sys.modules, {"akshare": _Fast()}):
            df, source, _errors = adapter._call_df_candidates(
                [("stock_individual_fund_flow", {"stock": "601398"})])
        self.assertIsNotNone(df)
        self.assertEqual(source, "stock_individual_fund_flow")


class TestContextStatus(unittest.TestCase):
    """`degraded` 必须一路透到 capital_flow 块，否则「带标记的降级」等于没标。"""

    def _context(self, payload):
        manager = DataFetcherManager(fetchers=[])
        cfg = SimpleNamespace(
            fundamental_fetch_timeout_seconds=2.0,
            fundamental_retry_max=1,
        )
        adapter = SimpleNamespace(get_capital_flow=lambda code: payload)
        with patch("src.config.get_config", return_value=cfg), \
                patch.object(manager, "_fundamental_adapter", adapter):
            return manager.get_capital_flow_context("601398")

    def test_degraded_payload_marks_block_degraded_and_keeps_source(self) -> None:
        block = self._context({
            "status": "partial",
            "stock_flow": {"main_net_inflow": 1.0, "inflow_5d": None, "inflow_10d": None},
            "sector_rankings": {"top": [], "bottom": []},
            "source_chain": ["capital_stock:ths_cache"],
            "errors": [],
            "stock_flow_source": "ths",
            "stock_flow_degraded": True,
            "stock_flow_metric": "net_inflow_all_orders",
        })
        self.assertEqual(block["status"], "degraded", "兜底源不许冒充 ok")
        self.assertEqual(block["data"]["stock_flow_source"], "ths")
        self.assertTrue(block["data"]["stock_flow_degraded"])
        self.assertEqual(block["data"]["stock_flow_metric"], "net_inflow_all_orders")

    def test_native_eastmoney_stays_ok(self) -> None:
        block = self._context({
            "status": "partial",
            "stock_flow": {"main_net_inflow": 1.0, "inflow_5d": 2.0, "inflow_10d": 3.0},
            "sector_rankings": {"top": [], "bottom": []},
            "source_chain": ["capital_stock:stock_individual_fund_flow"],
            "errors": [],
        })
        self.assertEqual(block["status"], "ok", "东财正常路径行为必须逐字不变")
        self.assertFalse(block["data"]["stock_flow_degraded"])


class TestZeroBudgetCacheFallback(unittest.TestCase):
    """2026-10-27 真机缺陷的回归：stage 预算被前面的阶段吃光 ⇒ capital_flow
    拿到 `budget_seconds=0`，旧实现在这里直接 `failed`，兜底代码执行不到
    （实测整段 8.02s、19 只票全部 `status=failed`）。

    读缓存是本地文件（毫秒级），不占预算 ⇒ 零预算下也必须能拿到缓存。
    """

    _FLOW = {"main_net_inflow": 618000000.0, "inflow_5d": None, "inflow_10d": None}

    def _manager(self):
        manager = DataFetcherManager(fetchers=[])
        cfg = SimpleNamespace(
            fundamental_fetch_timeout_seconds=8.0,
            fundamental_retry_max=1,
        )
        return manager, cfg

    def test_zero_budget_reads_cache_and_marks_degraded(self) -> None:
        manager, cfg = self._manager()
        adapter = SimpleNamespace(
            get_capital_flow=lambda code: (_ for _ in ()).throw(
                AssertionError("零预算下不该再走东财（预算已耗尽）"))
        )
        with patch("src.config.get_config", return_value=cfg), \
                patch.object(manager, "_fundamental_adapter", adapter), \
                patch("data_provider.fundamental_adapter.load_ths_fund_flow",
                      return_value=(dict(self._FLOW), None)):
            block = manager.get_capital_flow_context("601398", budget_seconds=0)
        self.assertEqual(block["status"], "degraded", "零预算兜底必须标 degraded，不冒充 ok")
        self.assertEqual(block["data"]["stock_flow"], self._FLOW)
        self.assertEqual(block["data"]["stock_flow_source"], "ths")
        self.assertTrue(block["data"]["stock_flow_degraded"])

    def test_zero_budget_without_cache_stays_failed(self) -> None:
        manager, cfg = self._manager()
        with patch("src.config.get_config", return_value=cfg), \
                patch("data_provider.fundamental_adapter.load_ths_fund_flow",
                      return_value=(None, "ths_cache_missing:ths_fund_flow_latest.json")):
            block = manager.get_capital_flow_context("601398", budget_seconds=0)
        self.assertEqual(block["status"], "failed", "没有缓存时保持原行为（不许伪造数据）")
        self.assertIn("fundamental stage timeout", block["errors"])


if __name__ == "__main__":
    unittest.main()
