#!/usr/bin/env python
# coding: utf-8
# author: snowman

import os
import polars as pl
from datetime import datetime, timezone
from common.env_com import env_mgr
from common.log_com import LogManager

logger = LogManager(name="cleaner").get_logger()


class BinanceCsvCleaner:
    """币安CSV文件清洗器"""

    def __init__(self, period, symbol, interval, date):
        self._base_dir = env_mgr.DATA_PATH
        self.interval_map = {"4h": 240, "2h": 120, "1h": 60, "30m": 30, "15m": 15, "5m": 5}
        self._cloumns = [
            "open_time", "open", "high", "low", "close", "volume", "close_time", "quote_asset_volume", 
            "number_of_trades", "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume", "ignore"
        ]
        self._period = period
        self._symbol = symbol
        self._interval = interval
        self._date = date
        self._df = self._load_all_csv()
        self._unit = self._detect_timestamp_unit()

    def _load_all_csv(self) -> pl.DataFrame:
        """读取指定目录下所有CSV文件"""
        path_dir = os.path.join(self._base_dir, "binance", "csv", "spot", self._period, "klines", self._symbol, self._interval, self._date)
        files = sorted([f for f in os.listdir(path_dir)])
        dfs = []
        for f in files:
            file_path = os.path.join(path_dir, f)
            df_part = pl.read_csv(file_path, has_header=False, new_columns=self._cloumns)
            dfs.append(df_part)
        combined = pl.concat(dfs).sort("open_time")
        return combined

    def _detect_timestamp_unit(self) -> int:
        """自动检测时间戳单位（秒 / 毫秒 / 微秒）"""
        first_ts = int(self._df["open_time"][0])
        ts_len = len(str(abs(first_ts)))
        if ts_len <= 10:
            unit = 1
        elif 11 <= ts_len <= 13:
            unit = 1000
        elif 14 <= ts_len <= 16:
            unit = 1000000
        else:
            raise ValueError(f"Unknown timestamp unit")
        return unit

    def _check_time_continuity(self):
        """检查时间连续性"""
        diffs = self._df["open_time"].diff().drop_nulls()
        expected = self.interval_map[self._interval] * 60 * self._unit
        discontinuous = self._df.filter(diffs != expected)
        if discontinuous.height > 0:
            raise ValueError(f"Detected discontinuous records.")

    def _check_null_values(self):
        """检查空值"""
        null_counts = self._df.null_count()
        has_null = any(count > 0 for count in null_counts)
        if has_null:
            raise ValueError(f"Detected null values")

    def _check_abnormal_values(self):
        """检查异常值"""
        invalid = self._df.filter((pl.col("open") <= 0) | (pl.col("high") <= 0) | (pl.col("low") <= 0) | (pl.col("close") <= 0) | (pl.col("volume") < 0) | (pl.col("high") < pl.col("low")))
        if invalid.height > 0:
            raise ValueError(f"Detected abnormal values")

    def _check_price_consistency(self):
        """价格一致性检查"""
        invalid = self._df.filter((pl.col("low") > pl.col("high")) | (pl.col("open") > pl.col("high")) | (pl.col("open") < pl.col("low")) | (pl.col("close") > pl.col("high")) | (pl.col("close") < pl.col("low")))
        if invalid.height > 0:
            raise ValueError(f"Detected price inconsistency")
    
    def _check_duplicate_timestamps(self):
        """重复时间戳检查"""
        dupes = self._df.group_by("open_time").agg(pl.count()).filter(pl.col("count") > 1)
        if dupes.height > 0:
            raise ValueError(f"Detected duplicate timestamps")

    def _convert_timestamp_to_utc(self):
        """兼容毫秒 / 微秒时间戳并转为 UTC"""
        def ts_to_iso(ts):
            dt = datetime.fromtimestamp(int(ts)/self._unit, tz=timezone.utc)
            return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        
        self._df = self._df.with_columns(
            pl.col("open_time").map_elements(ts_to_iso, return_dtype=pl.Utf8).alias("open_time_utc")
        )

    def _save_parquet(self):
        """把一年的CSV文件合并成Parquet文件"""
        filename = f"{self._symbol}-{self._interval}-{self._date}.parquet"
        path_dir = os.path.join(self._base_dir, "binance", "cleaned", "spot", "klines", f"timeframe={self._interval}", f"year={self._date}")
        file_path = os.path.join(path_dir, filename)
        os.makedirs(path_dir, exist_ok=True)
        self._df.write_parquet(file_path)

    def check_spot_klines_csv(self):
        """检查CSV文件完整性"""
        self._check_time_continuity()
        self._check_null_values()
        self._check_abnormal_values()
        self._check_price_consistency()
        self._check_duplicate_timestamps()
        self._convert_timestamp_to_utc()
        self._save_parquet()