# -*- coding: utf-8 -*-
"""FredFetcher - 美联储 FRED 宏观数据（regime 判断与 DCF rf 锚）。

免费 key: https://fred.stlouisfed.org/docs/api/api_key.html
序列：DGS10(10Y美债), CPIAUCSL(CPI), UNRATE(失业率), NAPM(ISM PMI)。
需配置 FRED_API_KEY，否则跳过；只做宏观兜底，不进 A 股主链。
"""
import logging
import os
from typing import Optional, Dict

import pandas as pd
import requests

from .base import BaseFetcher, DataFetchError

logger = logging.getLogger(__name__)

SERIES_IDS = ("DGS10", "CPIAUCSL", "UNRATE", "NAPM")


class FredFetcher(BaseFetcher):
    name = "FredFetcher"
    priority = int(os.getenv("FRED_PRIORITY", "6"))

    def __init__(self):
        self._api_key = os.getenv("FRED_API_KEY", "")
        self._enabled = bool(self._api_key)
        if self._enabled:
            logger.info("[FRED] 已启用")
        else:
            logger.debug("[FRED] 未配置 FRED_API_KEY，跳过")

    def _is_available(self) -> bool:
        return self._enabled

    def _fetch_raw_data(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        raise DataFetchError("[FRED] 仅提供宏观序列，不提供个股日线")

    def _normalize_data(self, df: pd.DataFrame, stock_code: str) -> pd.DataFrame:
        return df

    def _latest(self, series_id: str, timeout: int = 15) -> Optional[float]:
        try:
            resp = requests.get(
                "https://api.stlouisfed.org/fred/series/observations",
                params={"series_id": series_id, "api_key": self._api_key,
                        "file_type": "json", "sort_order": "desc", "limit": 5},
                timeout=timeout)
            resp.raise_for_status()
            for obs in resp.json().get("observations", []):
                try:
                    return float(obs["value"])
                except (KeyError, TypeError, ValueError):
                    continue
            return None
        except Exception as e:
            logger.warning(f"[FRED] {series_id} 获取失败: {e}")
            return None

    def get_market_stats(self) -> Optional[Dict]:
        """返回宏观四序列最新值，供 regime→strategy 与 DCF rf 锚使用。"""
        if not self._enabled:
            return None
        stats = {sid: self._latest(sid) for sid in SERIES_IDS}
        if all(v is None for v in stats.values()):
            return None
        stats["source"] = "fred"
        return stats
