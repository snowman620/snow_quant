#!/usr/bin/env python
# coding: utf-8
# author: snowman

import os
import polars as pl
from datetime import datetime, timezone
from common.env_com import env_mgr
from common.log_com import LogManager

logger = LogManager(name="cleaner").get_logger()

# pl.Config.set_tbl_rows(10_000_000)
# pl.Config.set_tbl_cols(10_000_000)


class BinanceCsvBase:
    """币安CSV文件基础类"""

    def __init__(self, period, symbol, interval):
        self._base_dir = env_mgr.DATA_PATH
        self._cloumns = [
            "open_time", "open", "high", "low", "close", "volume", "close_time", "quote_asset_volume", 
            "number_of_trades", "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume", "ignore"
        ]
        self.interval_map = {"4h": 14400, "2h": 7200, "1h": 3600, "30m": 1800, "15m": 900, "5m": 300}
        self._period = period
        self._symbol = symbol
        self._interval = interval
        self._csv_dir = os.path.join(self._base_dir, "binance", "csv", "spot", self._period, "klines", self._symbol, self._interval)
        self._cleaned_dir = os.path.join(self._base_dir, "binance", "cleaned", "spot", "klines", self._symbol, self._interval)


class BinanceCsvMerger(BinanceCsvBase):
    """币安CSV文件合并器"""

    def __init__(self, period, symbol, interval):
        super().__init__(period, symbol, interval)
        
    def merge_all_csv(self):
        """把每个月的CSV文件合并成一个整的CSV文件"""
        filename = f"{self._symbol}-{self._interval}.csv"
        os.makedirs(self._cleaned_dir, exist_ok=True)
        out_file_path = os.path.join(self._cleaned_dir, filename)
        combined = self._load_all_csv()
        combined.write_csv(out_file_path, include_header=False)

    def _load_all_csv(self) -> pl.DataFrame:
        """读取指定目录下所有CSV文件"""
        dfs = []
        files_2025_lt = []
        files_2025_gt = []
        for f in os.listdir(self._csv_dir):
            year = int(f.split("-")[2])
            if year < 2025:
                files_2025_lt.append(f)
            else:
                files_2025_gt.append(f)
        dfs.extend(self.merge_2025_lt(files_2025_lt))
        dfs.extend(self.merge_2025_gt(files_2025_gt))
        combined = pl.concat(dfs).sort("open_time")
        return combined
            
    def merge_2025_lt(self, files):
        """合并2025年之前的CSV文件"""
        dfs = []
        for f in sorted(files):
            file_path = os.path.join(self._csv_dir, f)
            df_part = pl.read_csv(file_path, has_header=False, new_columns=self._cloumns)
            
            df_part = df_part.with_columns([
                (pl.col("open_time") * 1000).cast(pl.Int64).alias("open_time"),
                (pl.col("close_time") * 1000 + 999).cast(pl.Int64).alias("close_time")
            ])
            dfs.append(df_part)
        return dfs
    
    def merge_2025_gt(self, files):
        """合并2025年之后的CSV文件"""
        dfs = []
        for f in sorted(files):
            file_path = os.path.join(self._csv_dir, f)
            df_part = pl.read_csv(file_path, has_header=False, new_columns=self._cloumns)
            dfs.append(df_part)
        return dfs


class BinanceCsvCleaner(BinanceCsvBase):
    """币安CSV文件清洗器"""

    def __init__(self, period, symbol, interval):
        super().__init__(period, symbol, interval)
        self._df = self._load_df()
    
    def _load_df(self):
        """加载DataFrame"""
        filename = os.listdir(self._cleaned_dir)[0]
        file_path = os.path.join(self._cleaned_dir, filename)
        df = pl.read_csv(file_path, has_header=False, new_columns=self._cloumns)
        return df
    
    def check_time_continuity(self):
        """识别时间不连续的行"""
        interval = self.interval_map[self._interval] * 1_000_000
        diffs = self._df["open_time"].diff().to_list()[1:]
        for i, d in enumerate(diffs):
            if d != interval:
                logger.error(f"第{i}行到第{i+1}行间隔 {d} ms")

    def fix_time_continuity(self):
        """线性插值"""
        filename = os.listdir(self._cleaned_dir)[0]
        filename_fixed = filename.replace(".csv", "-fix.csv")
        out_path = os.path.join(self._cleaned_dir, filename_fixed)

        if os.path.exists(out_path):
            logger.warning(f"已存在修复后的文件: {out_path}")
            return

        # 读取第一行和最后一行的open_time
        first_time = self._df["open_time"][0]
        last_time = self._df["open_time"][-1]
        interval = self.interval_map[self._interval] * 1_000_000

        # 生成完整时间序列（包含最后一个时间点）
        full_range = range(first_time, last_time + interval, interval)
        full_time = pl.DataFrame({"open_time": pl.Series(list(full_range), dtype=pl.Int64)})

        # 与原始数据左连接
        df_full = full_time.join(self._df, on="open_time", how="left")

        # 识别数值列（跳过时间戳列 open_time / close_time）
        numeric_cols = [
            c for c, dtype in zip(df_full.columns, df_full.dtypes)
            if c not in ("open_time", "close_time") and dtype in (pl.Float64, pl.Float32, pl.Int64, pl.Int32)
        ]

        # 对数值列做线性插值；单独处理 close_time（插值后四舍五入并转为 Int64），open_time 保证为 Int64
        exprs = []
        for c in df_full.columns:
            if c in ("open_time", "close_time"):
                continue
            if c in numeric_cols:
                exprs.append(pl.col(c).interpolate().alias(c))
            else:
                exprs.append(pl.col(c))

        # 确保时间戳为整型（对 close_time 做插值后 round -> Int64；open_time 直接 round -> Int64）
        exprs.append(pl.col("open_time").round().cast(pl.Int64).alias("open_time"))
        exprs.append(pl.col("close_time").interpolate().round().cast(pl.Int64).alias("close_time"))

        df_interp = df_full.with_columns(exprs)

        # 输出结果
        df_interp.write_csv(out_path, include_header=False)

    def check_null_values(self):
        """检查空值"""
        null_counts_obj = self._df.null_count()
        counts = list(null_counts_obj.row(0))
        null_map = {c: int(n) for c, n in zip(self._df.columns, counts) if int(n) > 0}
        if null_map:
            raise ValueError(f"发现空值: {null_map}")

    def check_abnormal_values(self):
        """检查异常值"""
        invalid = self._df.filter((pl.col("open") <= 0) | (pl.col("high") <= 0) | (pl.col("low") <= 0) | (pl.col("close") <= 0) | (pl.col("volume") < 0) | (pl.col("high") < pl.col("low")))
        if invalid.height > 0:
            raise ValueError(f"发现 {invalid.height} 个异常值")

    def check_price_consistency(self):
        """价格一致性检查"""
        invalid = self._df.filter((pl.col("low") > pl.col("high")) | (pl.col("open") > pl.col("high")) | (pl.col("open") < pl.col("low")) | (pl.col("close") > pl.col("high")) | (pl.col("close") < pl.col("low")))
        if invalid.height > 0:
            raise ValueError(f"发现 {invalid.height} 个异常价格")
    
    def check_duplicate_timestamps(self):
        """重复时间戳检查"""
        dupes = self._df.group_by("open_time").agg(pl.len()).filter(pl.col("len") > 1)
        if dupes.height > 0:
            raise ValueError(f"发现 {dupes.height} 个重复时间戳")

    # def _save_parquet(self):
    #     """把CSV文件转成Parquet文件"""
    #     filename = f"{self._symbol}-{self._interval}.parquet"
    #     path_dir = os.path.join(self._base_dir, "binance", "cleaned", "spot", "klines", self._symbol, self._interval)
    #     file_path = os.path.join(path_dir, filename)
    #     os.makedirs(path_dir, exist_ok=True)
    #     self._df.write_parquet(file_path)


if __name__ == "__main__":
    cleaner = BinanceCsvCleaner(period="monthly", symbol="BTCUSDT", interval="4h")
    cleaner.check_time_continuity()
    cleaner.fix_time_continuity()
    cleaner.check_null_values()
    cleaner.check_abnormal_values()
    cleaner.check_price_consistency()
    cleaner.check_duplicate_timestamps()
