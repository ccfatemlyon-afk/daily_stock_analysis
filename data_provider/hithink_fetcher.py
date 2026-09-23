"""
同花顺 Financial-API 数据源

官方文档: https://fuyao.aicubes.cn/docs/
API Key: https://fuyao.aicubes.cn/admin/

支持数据:
- A股实时行情、历史K线
- 财务报表、估值数据
- 指数、板块、涨跌停
- 集合竞价、龙虎榜
"""

import logging
import os
from typing import Optional

import pandas as pd
import requests

from .base import BaseFetcher, DataFetchError

logger = logging.getLogger(__name__)


class HiThinkFetcher(BaseFetcher):
    """同花顺 Financial-API 数据源"""

    name = "HiThinkFetcher"
    priority = int(os.getenv("HITHINK_PRIORITY", "6"))

    def __init__(self):
        self._api_key = os.getenv("HITHINK_FINANCE_API_KEY", "")
        self._base_url = os.getenv("HITHINK_BASE_URL", "https://fuyao.aicubes.cn/api")
        self._enabled = bool(self._api_key)
        if self._enabled:
            self.priority = int(os.getenv("HITHINK_PRIORITY", "1"))
            logger.info(f"[HiThink] 已启用，优先级={self.priority}")
        else:
            logger.debug("[HiThink] 未配置 HITHINK_FINANCE_API_KEY，跳过")

    def _request(self, path: str, params: dict, timeout: int = 30) -> Optional[dict]:
        """发送 API 请求"""
        if not self._enabled:
            return None
        try:
            headers = {"X-api-key": self._api_key}
            resp = requests.get(f"{self._base_url}{path}", params=params, headers=headers, timeout=timeout)
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, dict) and data.get("error"):
                logger.warning(f"[HiThink] API 错误: {data['error']}")
                return None
            return data
        except Exception as e:
            logger.warning(f"[HiThink] 请求失败: {e}")
            return None

    def _is_available(self) -> bool:
        return self._enabled

    def _fetch_raw_data(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取原始日K数据"""
        if not self._enabled:
            raise DataFetchError("[HiThink] 未配置 HITHINK_FINANCE_API_KEY")

        # 转换为同花顺代码格式
        thscode = self._to_thscode(stock_code)

        data = self._request("/a-share/kline", {
            "thscode": thscode,
            "start": start_date.replace("-", ""),
            "end": end_date.replace("-", ""),
            "period": "day",
        })

        if not data or not isinstance(data, (list, dict)):
            raise DataFetchError(f"[HiThink] 未获取到 {stock_code} 的K线数据")

        records = data if isinstance(data, list) else data.get("data", data.get("klines", []))
        if not records:
            raise DataFetchError(f"[HiThink] {stock_code} K线数据为空")

        df = pd.DataFrame(records)
        return df

    def _normalize_data(self, df: pd.DataFrame, stock_code: str) -> pd.DataFrame:
        """标准化列名"""
        column_map = {
            "date": "date",
            "日期": "date",
            "trade_date": "date",
            "open": "open",
            "开盘": "open",
            "high": "high",
            "最高": "high",
            "low": "low",
            "最低": "low",
            "close": "close",
            "收盘": "close",
            "volume": "volume",
            "成交量": "volume",
            "amount": "amount",
            "成交额": "amount",
            "pct_chg": "pct_chg",
            "涨跌幅": "pct_chg",
        }

        df = df.rename(columns={k: v for k, v in column_map.items() if k in df.columns})

        for col in ["date", "open", "high", "low", "close", "volume"]:
            if col not in df.columns:
                raise DataFetchError(f"[HiThink] 缺少必要列: {col}")

        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df["date"] = pd.to_datetime(df["date"])

        if "amount" not in df.columns:
            df["amount"] = 0
        if "pct_chg" not in df.columns:
            df["pct_chg"] = df["close"].pct_change() * 100

        df = df.sort_values("date").reset_index(drop=True)
        return df

    def _to_thscode(self, stock_code: str) -> str:
        """转换股票代码为同花顺格式 (600519.SH)"""
        code = stock_code.upper().replace("HK", "").replace("SH", "").replace("SZ", "")
        code = code.split(".")[0].strip()

        if code.startswith("6"):
            return f"{code}.SH"
        elif code.startswith("0") or code.startswith("3"):
            return f"{code}.SZ"
        elif code.startswith("4") or code.startswith("8"):
            return f"{code}.BJ"
        return f"{code}.SH"

    def get_realtime_quote(self, stock_code: str):
        """获取实时行情"""
        if not self._enabled:
            return None
        try:
            thscode = self._to_thscode(stock_code)
            data = self._request("/a-share/prices/snapshot", {"thscodes": thscode})
            if not data:
                return None

            record = data if isinstance(data, dict) else {}
            if isinstance(data, list) and data:
                record = data[0]

            from .realtime_types import UnifiedRealtimeQuote, RealtimeSource
            return UnifiedRealtimeQuote(
                code=stock_code,
                name=record.get("name", ""),
                price=float(record.get("close", 0)),
                change_pct=float(record.get("pct_chg", 0)),
                volume=float(record.get("volume", 0)),
                amount=float(record.get("amount", 0)),
                open=float(record.get("open", 0)),
                high=float(record.get("high", 0)),
                low=float(record.get("low", 0)),
                pre_close=float(record.get("pre_close", 0)),
                source=RealtimeSource.OTHER,
            )
        except Exception as e:
            logger.warning(f"[HiThink] 实时行情获取失败: {e}")
            return None

    def get_stock_name(self, stock_code: str) -> Optional[str]:
        """获取股票名称"""
        if not self._enabled:
            return None
        try:
            thscode = self._to_thscode(stock_code)
            data = self._request("/a-share/symbol/search", {"q": thscode, "limit": 1})
            if data and isinstance(data, list) and data:
                return data[0].get("name")
            return None
        except Exception:
            return None
