"""
XTick 数据源 - 基于 XTick API 的股票数据获取

API 文档: http://www.xtick.top/apidoc
需要注册获取 Token: http://www.xtick.top/user/account

支持数据:
- 沪深京A股、ETF基金、港股
- 日K线、分钟K线
- 技术指标 (MACD, RSI, KDJ, BOLL 等 157 种)
- 实时行情、财务数据
"""

import logging
import os
import time
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
import requests

from .base import BaseFetcher, DataFetchError

logger = logging.getLogger(__name__)


class XTickFetcher(BaseFetcher):
    """XTick API 数据源"""

    name = "XTickFetcher"
    priority = int(os.getenv("XTICK_PRIORITY", "6"))  # 默认低优先级，需配置 token 后提升

    def __init__(self):
        self._token = os.getenv("XTICK_TOKEN", "")
        self._base_url = os.getenv("XTICK_BASE_URL", "http://api.xtick.top")
        self._enabled = bool(self._token)
        if self._enabled:
            self.priority = int(os.getenv("XTICK_PRIORITY", "1"))  # 有 token 时提升优先级
            logger.info(f"[XTick] 已启用，优先级={self.priority}")
        else:
            logger.debug("[XTick] 未配置 XTICK_TOKEN，跳过")

    def _request(self, path: str, params: dict, timeout: int = 30) -> Optional[dict]:
        """发送 API 请求"""
        if not self._enabled:
            return None
        try:
            params["token"] = self._token
            resp = requests.get(f"{self._base_url}{path}", params=params, timeout=timeout)
            resp.raise_for_status()
            data = resp.json()
            if data.get("code") == 200 or data.get("success"):
                return data.get("data", data)
            logger.warning(f"[XTick] API 返回错误: {data.get('msg', data)}")
            return None
        except Exception as e:
            logger.warning(f"[XTick] 请求失败: {e}")
            return None

    def _is_available(self) -> bool:
        return self._enabled

    def _fetch_raw_data(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取原始日K数据"""
        if not self._enabled:
            raise DataFetchError("[XTick] 未配置 XTICK_TOKEN")

        # XTick 格式: type=1 为沪深京A股, type=2 为港股
        stock_type = "1"
        code = stock_code
        if stock_code.upper().startswith("HK"):
            stock_type = "2"
            code = stock_code[2:]

        data = self._request("/api/stock/kline", {
            "type": stock_type,
            "code": code,
            "fq": "1",  # 前复权
            "period": "1d",
            "startDate": start_date,
            "endDate": end_date,
        })

        if not data or not isinstance(data, list):
            raise DataFetchError(f"[XTick] 未获取到 {stock_code} 的K线数据")

        df = pd.DataFrame(data)
        return df

    def _normalize_data(self, df: pd.DataFrame, stock_code: str) -> pd.DataFrame:
        """标准化列名"""
        column_map = {
            "date": "date",
            "日期": "date",
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

        # 确保必要列存在
        for col in ["date", "open", "high", "low", "close", "volume"]:
            if col not in df.columns:
                raise DataFetchError(f"[XTick] 缺少必要列: {col}")

        # 类型转换
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df["date"] = pd.to_datetime(df["date"])

        if "amount" not in df.columns:
            df["amount"] = 0
        if "pct_chg" not in df.columns:
            df["pct_chg"] = df["close"].pct_change() * 100

        df = df.sort_values("date").reset_index(drop=True)
        return df

    def get_realtime_quote(self, stock_code: str):
        """获取实时行情"""
        if not self._enabled:
            return None
        try:
            stock_type = "1"
            code = stock_code
            if stock_code.upper().startswith("HK"):
                stock_type = "2"
                code = stock_code[2:]

            data = self._request("/api/stock/realtime", {
                "type": stock_type,
                "code": code,
            })

            if not data:
                return None

            from .realtime_types import UnifiedRealtimeQuote, RealtimeSource
            return UnifiedRealtimeQuote(
                code=stock_code,
                name=data.get("name", ""),
                price=float(data.get("close", 0)),
                change_pct=float(data.get("pct_chg", 0)),
                volume=float(data.get("volume", 0)),
                amount=float(data.get("amount", 0)),
                open=float(data.get("open", 0)),
                high=float(data.get("high", 0)),
                low=float(data.get("low", 0)),
                pre_close=float(data.get("pre_close", 0)),
                source=RealtimeSource.OTHER,
            )
        except Exception as e:
            logger.warning(f"[XTick] 实时行情获取失败: {e}")
            return None

    def get_stock_name(self, stock_code: str) -> Optional[str]:
        """获取股票名称"""
        if not self._enabled:
            return None
        try:
            data = self._request("/api/stock/info", {"code": stock_code})
            return data.get("name") if data else None
        except Exception:
            return None
