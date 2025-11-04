#!/usr/bin/env python
# coding: utf-8
# author: snowman

import os
import polars as pl
from common.env_com import env_mgr
from common.log_com import LogManager

logger = LogManager(name="cleaner").get_logger()


class BinanceCsvBase:
    """币安CSV文件基础类"""

    def __init__(self, period, symbol, interval):
        self._base_dir = env_mgr.DATA_PATH
        self._cloumns = [
            "open_time", "open", "high", "low", "close", "volume", "close_time", "quote_asset_volume", 
            "number_of_trades", "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume", "ignore"
        ]
        self._schemas = {
            "open_time": pl.Int64,
            "open": pl.Float64,
            "high": pl.Float64,
            "low": pl.Float64,
            "close": pl.Float64,
            "volume": pl.Float64,
            "close_time": pl.Int64,
            "quote_asset_volume": pl.Float64,
            "number_of_trades": pl.Float64,
            "taker_buy_base_asset_volume": pl.Float64,
            "taker_buy_quote_asset_volume": pl.Float64,
            "ignore": pl.Int64
        }
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
            df_part = pl.read_csv(file_path, has_header=False, new_columns=self._cloumns, schema=self._schemas)
            
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
            df_part = pl.read_csv(file_path, has_header=False, new_columns=self._cloumns, schema=self._schemas)
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
        df = pl.read_csv(file_path, has_header=False, new_columns=self._cloumns, schema=self._schemas)
        return df
    
    def _check_time_continuity(self):
        """识别时间不连续的行"""
        interval = self.interval_map[self._interval] * 1_000_000
        diffs = self._df["open_time"].diff().to_list()[1:]
        for i, d in enumerate(diffs):
            if d != interval:
                raise ValueError(f"{self._interval} 第{i}行到第{i+1}行间隔 {d} ms")
    
    def _check_null_values(self):
        """检查空值"""
        null_counts_obj = self._df.null_count()
        counts = list(null_counts_obj.row(0))
        null_map = {c: int(n) for c, n in zip(self._df.columns, counts) if int(n) > 0}
        if null_map:
            raise ValueError(f"发现空值: {null_map}")

    def _check_abnormal_values(self):
        """检查异常值"""
        invalid = self._df.filter((pl.col("open") <= 0) | (pl.col("high") <= 0) | (pl.col("low") <= 0) | (pl.col("close") <= 0) | (pl.col("volume") < 0) | (pl.col("high") < pl.col("low")))
        if invalid.height > 0:
            raise ValueError(f"发现 {invalid.height} 个异常值")

    def _check_price_consistency(self):
        """价格一致性检查"""
        invalid = self._df.filter((pl.col("low") > pl.col("high")) | (pl.col("open") > pl.col("high")) | (pl.col("open") < pl.col("low")) | (pl.col("close") > pl.col("high")) | (pl.col("close") < pl.col("low")))
        if invalid.height > 0:
            raise ValueError(f"发现 {invalid.height} 个异常价格")
    
    def _check_duplicate_timestamps(self):
        """重复时间戳检查"""
        dupes = self._df.group_by("open_time").agg(pl.len()).filter(pl.col("len") > 1)
        if dupes.height > 0:
            raise ValueError(f"发现 {dupes.height} 个重复时间戳")

    def fix_time_continuity(self):
        """线性插值"""
        filename = os.listdir(self._cleaned_dir)[0]
        out_path = os.path.join(self._cleaned_dir, filename)

        # if os.path.exists(out_path):
        #     logger.warning(f"已存在修复后的文件: {out_path}")
        #     return

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

        # 线性插值后，把 number_of_trades 全部转为 Float64
        self._df = self._df.with_columns(pl.col("number_of_trades").cast(pl.Float64))

        # 直接把 ignore 列设为整型常量 0
        df_interp = df_interp.with_columns(pl.lit(0).cast(pl.Int64).alias("ignore"))

        # 输出结果
        df_interp.write_csv(out_path, include_header=False)

    def check_spot_klines_csv(self):
        """检查CSV文件内容是否合理"""
        self._check_time_continuity()
        self._check_null_values()
        self._check_abnormal_values()
        self._check_price_consistency()
        self._check_duplicate_timestamps()


if __name__ == "__main__":
    intervals = ["4h", "2h", "1h", "30m", "15m", "5m"]
    
    # 先合并
    # for i in intervals:
    #     merger = BinanceCsvMerger("monthly", "BTCUSDT", i)
    #     merger.merge_all_csv()
    
    # 再检测
    # for i in intervals:
    #     cleaner = BinanceCsvCleaner("monthly", "BTCUSDT", i)
    #     cleaner.check_spot_klines_csv()
    
    # 最后线性插值，没问题的也要执行，需要转换数值类型
    # for i in intervals:
    #     cleaner = BinanceCsvCleaner("monthly", "BTCUSDT", i)
    #     cleaner.fix_time_continuity()
