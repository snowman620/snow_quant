#!/usr/bin/env python
# coding: utf-8
# author: snowman

import os
import polars as pl
import numpy as np
from common.env_com import env_mgr
from common.log_com import LogManager

logger = LogManager(name="analyst").get_logger()

# pl.Config.set_tbl_rows(10_000_000)
# pl.Config.set_tbl_cols(10_000_000)


class BinanceCsvAnalyst:
    """多周期币安CSV分析器 + 特征工程"""
    def __init__(self, symbol, interval):
        self._base_dir = env_mgr.DATA_PATH
        self._symbol = symbol
        self._interval = interval
        self._cleaned_dir = os.path.join(self._base_dir, "binance", "cleaned", "spot", "klines", self._symbol, self._interval)
        self._featured_dir = os.path.join(self._base_dir, "binance", "featured", "spot", "klines", self._symbol, self._interval)
        self._confs = self._get_conf()
        self._df = self._load_csv()

    def _get_conf(self):
        """获取配置参数"""
        return {
            "4h": {"ema_fast": 21, "ema_slow": 55, "macd": (12, 26, 9), "rsi": 14},
            "2h": {"ema_fast": 26, "ema_slow": 78, "macd": (12, 26, 9), "rsi": 14},
            "1h": {"ema_fast": 12, "ema_slow": 26, "macd": (12, 26, 9), "rsi": 14},
            "30m": {"ema_fast": 12, "ema_slow": 26, "macd": (9, 21, 9), "rsi": 9},
            "15m": {"ema_fast": 9, "ema_slow": 21, "macd": (7, 14, 7), "rsi": 7},
            "5m": {"ema_fast": 7, "ema_slow": 14, "macd": (5, 13, 5), "rsi": 6},
        }[self._interval]

    def _load_csv(self):
        """加载清洗后的CSV文件"""
        columns = [
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_asset_volume", "number_of_trades",
            "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume", "ignore"
        ]
        schemas = {
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
        filename = f"{self._symbol}-{self._interval}.csv"
        path = os.path.join(self._cleaned_dir, filename)
        return pl.read_csv(path, has_header=False, new_columns=columns, schema=schemas)

    def drop_ignore_column(self):
        """删除无用的ignore列"""
        self._df = self._df.drop("ignore")

    def convert_open_time_to_utc(self):
        """转换开盘时间为UTC时间"""
        self._df = self._df.with_columns(
            pl.col("open_time").cast(pl.Datetime("us")).alias("open_time_utc")
        )

    def add_label(self):
        """添加涨跌标签列"""
        self._df = self._df.with_columns(
            (pl.col("close").shift(-1) > pl.col("close")).cast(pl.Int8).alias("label")
        ).drop_nulls(subset=["label"])

    def add_candlestick_features(self):
        """添加蜡烛形态特征"""
        
        # 蜡烛结构基础量
        body = (pl.col("close") - pl.col("open")).abs()
        upper_shadow = pl.col("high") - pl.max_horizontal(["close", "open"])
        lower_shadow = pl.min_horizontal(["close", "open"]) - pl.col("low")
        body_ratio = body / (pl.col("high") - pl.col("low") + 1e-12)

        # 单根形态
        self._df = self._df.with_columns([
            # 十字星：实体极短，上下影线较长
            ((body_ratio < 0.1) & ((upper_shadow > body * 2) & (lower_shadow > body * 2))).cast(pl.Int8).alias("doji"),
            # 锤子线：下影线很长，实体靠上
            ((lower_shadow > body * 2) & (upper_shadow < body)).cast(pl.Int8).alias("hammer"),
            # 上吊线（倒锤子）
            ((upper_shadow > body * 2) & (lower_shadow < body)).cast(pl.Int8).alias("inverted_hammer"),
            # 光头光脚大阳线 / 大阴线
            ((body_ratio > 0.8) & (pl.col("close") > pl.col("open"))).cast(pl.Int8).alias("marubozu_bull"),
            ((body_ratio > 0.8) & (pl.col("close") < pl.col("open"))).cast(pl.Int8).alias("marubozu_bear"),
        ])

        # 双根形态
        self._df = self._df.with_columns([
            # 吞没形态：后K完全包住前K
            ((pl.col("close") > pl.col("open")) & 
             (pl.col("close").shift(1) < pl.col("open").shift(1)) &
             (pl.col("close") > pl.col("open").shift(1)) &
             (pl.col("open") < pl.col("close").shift(1))).cast(pl.Int8).alias("bullish_engulfing"),

            ((pl.col("close") < pl.col("open")) &
             (pl.col("close").shift(1) > pl.col("open").shift(1)) &
             (pl.col("close") < pl.col("open").shift(1)) &
             (pl.col("open") > pl.col("close").shift(1))).cast(pl.Int8).alias("bearish_engulfing"),
        ])

        # 三根形态：晨星、黄昏星（简单判定）
        self._df = self._df.with_columns([
            # 晨星：阴 -> 十字/小阳 -> 大阳
            ((pl.col("close").shift(2) < pl.col("open").shift(2)) &
             (body_ratio.shift(1) < 0.2) &
             (pl.col("close") > pl.col("open"))).cast(pl.Int8).alias("morning_star"),

            # 黄昏星：阳 -> 十字/小阴 -> 大阴
            ((pl.col("close").shift(2) > pl.col("open").shift(2)) &
             (body_ratio.shift(1) < 0.2) &
             (pl.col("close") < pl.col("open"))).cast(pl.Int8).alias("evening_star"),
        ])

    def add_technical_indicators(self):
        """添加技术指标特征"""
        p = self._confs

        # 移动均线
        self._df = self._df.with_columns([
            pl.col("close").rolling_mean(window_size=p["ema_fast"]).alias(f"sma_{p['ema_fast']}"),
            pl.col("close").rolling_mean(window_size=p["ema_slow"]).alias(f"sma_{p['ema_slow']}"),
        ])

        # EMA计算
        def ema(series, span):
            alpha = 2 / (span + 1)
            out = [series[0]]
            for price in series[1:]:
                out.append(out[-1] * (1 - alpha) + price * alpha)
            return out

        close_np = self._df["close"].to_numpy()
        ema_fast = ema(close_np, p["ema_fast"])
        ema_slow = ema(close_np, p["ema_slow"])
        self._df = self._df.with_columns([
            pl.Series(f"ema_{p['ema_fast']}", ema_fast),
            pl.Series(f"ema_{p['ema_slow']}", ema_slow)
        ])

        # MACD
        fast, slow, sig = p["macd"]
        macd_line = np.array(ema(close_np, fast)) - np.array(ema(close_np, slow))
        signal = ema(macd_line, sig)
        hist = macd_line - np.array(signal)
        self._df = self._df.with_columns([
            pl.Series("macd", macd_line),
            pl.Series("macd_signal", signal),
            pl.Series("macd_hist", hist),
            (pl.Series(macd_line) > pl.Series(signal)).cast(pl.Int8).alias("macd_cross_up"),
            (pl.Series(macd_line) < pl.Series(signal)).cast(pl.Int8).alias("macd_cross_down"),
        ])

        # RSI
        delta = np.diff(close_np, prepend=close_np[0])
        gain = np.where(delta > 0, delta, 0)
        loss = np.where(delta < 0, -delta, 0)

        def rsi(gain, loss, period):
            avg_gain = np.convolve(gain, np.ones(period), 'valid') / period
            avg_loss = np.convolve(loss, np.ones(period), 'valid') / period
            rs = avg_gain / (avg_loss + 1e-10)
            rsi_val = 100 - (100 / (1 + rs))
            return np.concatenate([np.full(period - 1, np.nan), rsi_val])

        rsi_vals = rsi(gain, loss, p["rsi"])
        self._df = self._df.with_columns([
            pl.Series(f"rsi_{p['rsi']}", rsi_vals),
            (pl.Series(rsi_vals) > 70).cast(pl.Int8).alias("rsi_overbought"),
            (pl.Series(rsi_vals) < 30).cast(pl.Int8).alias("rsi_oversold"),
        ])

    def add_combined_features(self):
        """添加组合特征"""
        fast = self._confs["ema_fast"]
        slow = self._confs["ema_slow"]
        rsi_period = self._confs["rsi"]

        self._df = self._df.with_columns([
            ((pl.col(f"sma_{fast}") > pl.col(f"sma_{slow}")) &
             (pl.col(f"ema_{fast}") > pl.col(f"ema_{slow}"))).cast(pl.Int8).alias("ma_bullish"),
            ((pl.col(f"rsi_{rsi_period}") < 40) & (pl.col("macd_cross_up") == 1)).cast(pl.Int8).alias("rsi_macd_combo"),
            ((pl.col("hammer") == 1) & (pl.col(f"rsi_{rsi_period}") < 40)).cast(pl.Int8).alias("hammer_low_rsi"),
        ])

    def add_volume_features(self):
        """成交量与订单流特征提取"""

        # === 1成交量与成交额 ===
        # 均量比：当前成交量 / 过去N根均量（短期动能）
        self._df = self._df.with_columns([
            (pl.col("volume") / pl.col("volume").shift(5).rolling_mean(window_size=5))
            .alias("vol_ma_ratio_5"),  # 5根均量比

            (pl.col("quote_asset_volume") / pl.col("quote_asset_volume").shift(5).rolling_mean(window_size=5))
            .alias("quote_vol_ma_ratio_5"),  # 成交额均量比
        ])

        # 成交量变化率（衡量量能变化速度）
        self._df = self._df.with_columns([
            ((pl.col("volume") - pl.col("volume").shift(1)) / (pl.col("volume").shift(1) + 1e-12))
            .alias("vol_change_rate"),
        ])

        # === 2主动买盘 vs 被动卖盘 ===
        # 主动买入比例（订单流强度）
        self._df = self._df.with_columns([
            (pl.col("taker_buy_base_asset_volume") / (pl.col("volume") + 1e-12))
            .alias("buy_ratio_base"),  # 主动买入量占比

            (pl.col("taker_buy_quote_asset_volume") / (pl.col("quote_asset_volume") + 1e-12))
            .alias("buy_ratio_quote"),  # 主动买入额占比
        ])

        # 买卖力量差值
        self._df = self._df.with_columns([
            (pl.col("buy_ratio_base") - 0.5).alias("buy_pressure"),  # >0 偏多，<0 偏空
        ])

        # === 3成交活跃度 ===
        # 成交笔数变化与成交量变化的配合
        self._df = self._df.with_columns([
            (pl.col("number_of_trades") / (pl.col("number_of_trades").shift(5).rolling_mean(window_size=5) + 1e-12))
            .alias("trade_count_ma_ratio"),

            ((pl.col("number_of_trades") - pl.col("number_of_trades").shift(1)) /
            (pl.col("number_of_trades").shift(1) + 1e-12))
            .alias("trade_count_change_rate"),
        ])

        # === 4价格与量能的共振特征 ===
        # 价格涨跌与成交量变化是否同向
        self._df = self._df.with_columns([
            (((pl.col("close") - pl.col("close").shift(1)) > 0).cast(pl.Int8()) * 
            ((pl.col("volume") - pl.col("volume").shift(1)) > 0).cast(pl.Int8()))
            .alias("price_volume_up_sync"),  # 同向上升信号 (1=同涨)
        ])

        # 价格变化 × 成交量变化（动能强度）
        self._df = self._df.with_columns([
            ((pl.col("close") - pl.col("close").shift(1)) * 
            (pl.col("volume") - pl.col("volume").shift(1)))
            .alias("price_volume_power"),
        ])

        # === 5成交能量指标 ===
        # quote_asset_volume 实际上是成交额（volume * 价格）
        # 可以反映市场能量变化
        self._df = self._df.with_columns([
            (pl.col("quote_asset_volume") / (pl.col("quote_asset_volume").shift(1) + 1e-12) - 1)
            .alias("quote_volume_change_rate"),

            (pl.col("close") * pl.col("volume")).alias("turnover_power"),  # 成交能量
        ])

    def save_featured(self):
        """保存特征工程后的数据"""
        os.makedirs(self._featured_dir, exist_ok=True)
        outfile = os.path.join(self._featured_dir, f"{self._symbol}-{self._interval}.parquet")
        self._df.write_parquet(outfile)
        # outfile = os.path.join(self._featured_dir, f"{self._symbol}-{self._interval}.csv")
        # self._df.write_csv(outfile)
        
    def analyze(self):
        self.drop_ignore_column()
        self.convert_open_time_to_utc()
        self.add_label()
        self.add_candlestick_features()
        self.add_technical_indicators()
        self.add_combined_features()
        self.add_volume_features()
        self.save_featured()


if __name__ == "__main__":
    intervals = ["4h", "2h", "1h", "30m", "15m", "5m"]
    # for i in intervals:
    #     analyst = BinanceCsvAnalyst("BTCUSDT", i)
    #     analyst.analyze()
