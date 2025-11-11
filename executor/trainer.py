#!/usr/bin/env python
# coding: utf-8
# author: snowman

import os
import polars as pl
import numpy as np
import lightgbm as lgb
import matplotlib.pyplot as plt
from sklearn.metrics import roc_auc_score, roc_curve
from common.env_com import env_mgr
from common.log_com import LogManager

logger = LogManager(name="trainer").get_logger()


class BinanceTrainer:
    """币安模型训练器（使用Polars + LightGBM）"""

    def __init__(self, symbol, interval, mode):
        self._base_dir = env_mgr.DATA_PATH
        self._symbol = symbol
        self._interval = interval
        self._mode = mode
        self._featured_dir = os.path.join(self._base_dir, "binance", "featured", "spot", "klines", self._symbol, self._interval)
        self._parquet_path = os.path.join(self._featured_dir, f"{symbol}-{interval}.parquet")
        self._cloumns = self._load_columns()
        self._df = self._load_parquet()

    def _get_conf(self):
        """获取配置参数"""
        return {
            "4h": ["sma_21", "sma_55", "ema_21", "ema_55", "rsi_14"],
            "2h": ["sma_26", "sma_78", "ema_26", "ema_78", "rsi_14"],
            "1h": ["sma_12", "sma_26", "ema_12", "ema_26", "rsi_14"],
            "30m": ["sma_12", "sma_26", "ema_12", "ema_26", "rsi_9"],
            "15m": ["sma_9", "sma_21", "ema_9", "ema_21", "rsi_7"],
            "5m": ["sma_7", "sma_14", "ema_7", "ema_14", "rsi_6"],
        }[self._interval]

    def _load_columns(self):
        """根据mode加载列"""
        # "open_time", "open_time_utc", 
        if self._mode == "shape":
            return self._get_conf() + ["doji", "hammer", "inverted_hammer", "marubozu_bull", "marubozu_bear", "bullish_engulfing", "bearish_engulfing", "morning_star", "evening_star", "macd", "macd_signal", "macd_hist", "macd_cross_up", "macd_cross_down", "rsi_overbought", "rsi_oversold", "ma_bullish", "rsi_macd_combo", "hammer_low_rsi"]
        elif self._mode == "volume":
            return ["vol_ma_ratio_5", "quote_vol_ma_ratio_5", "vol_change_rate", "buy_ratio_base", "buy_ratio_quote", "buy_pressure", "trade_count_ma_ratio", "trade_count_change_rate", "price_volume_up_sync", "price_volume_power", "quote_volume_change_rate", "turnover_power"]
        else:
            return []

    def _load_parquet(self):
        """加载parquet数据"""
        columns = self._cloumns + ["label"]
        df = pl.read_parquet(self._parquet_path, columns=columns)
        # df.sort("open_time")
        return df

    def time_split(self, train_ratio: float = 0.8):
        """数据拆分（时间序列划分）"""
        split_idx = int(self._df.height * train_ratio)
        df_train = self._df.slice(0, split_idx)
        df_valid = self._df.slice(split_idx, self._df.height - split_idx)
        return df_train, df_valid

    def train_lightgbm(self, params: dict | None = None):
        """模型训练"""
        df_train, df_valid = self.time_split()
        X_train = df_train.select(self._cloumns).to_numpy()
        y_train = df_train["label"].to_numpy()
        X_valid = df_valid.select(self._cloumns).to_numpy()
        y_valid = df_valid["label"].to_numpy()

        default_params = {
            "objective": "binary",
            "metric": "auc",
            "boosting_type": "gbdt",
            "verbosity": -1,
            "num_leaves": 63,
            "learning_rate": 0.01,
            "feature_fraction": 0.9,
            "bagging_fraction": 0.8,
            "bagging_freq": 5,
        }
        if params:
            default_params.update(params)

        # 定义回调
        callbacks = [
            lgb.early_stopping(stopping_rounds=100),
            lgb.log_evaluation(period=100)
        ]

        train_data = lgb.Dataset(X_train, label=y_train, feature_name=self._cloumns)
        valid_data = lgb.Dataset(X_valid, label=y_valid, feature_name=self._cloumns)

        model = lgb.train(
            default_params,
            train_data,
            valid_sets=[train_data, valid_data],
            num_boost_round=2000,
            callbacks=callbacks,
        )

        y_pred = np.asarray(model.predict(X_valid, num_iteration=model.best_iteration))
        auc = roc_auc_score(y_valid, y_pred)
        print(f"\n训练完成 | 验证集 AUC: {auc:.4f}")

        self.plot_feature_importance(model, self._cloumns)
        self.plot_roc(y_valid, y_pred)

        return model

    def plot_feature_importance(self, model, feature_names):
        """特征重要性"""
        importance = model.feature_importance()
        plt.figure(figsize=(10, 6))
        plt.barh(feature_names, importance)
        plt.title("LightGBM Feature Importance")
        plt.tight_layout()
        # 直接打开图片
        # plt.show()
        # 保存图片
        # plt.savefig(os.path.join(self._featured_dir,"feature_importance.png"), dpi=300, bbox_inches="tight")
        # plt.close()
        # 直接打印
        features = model.feature_name()
        imp_df = sorted(zip(features, importance), key=lambda x: x[1], reverse=True)
        print("\nTop Feature Importance:")
        for name, val in imp_df[:20]:
            print(f"{name:<30} {val:>10.2f}")


    def plot_roc(self, y_true, y_pred):
        """变动率"""
        fpr, tpr, _ = roc_curve(y_true, y_pred)
        plt.figure(figsize=(6, 5))
        plt.plot(fpr, tpr, label=f"ROC curve (AUC = {roc_auc_score(y_true, y_pred):.3f})")
        plt.plot([0, 1], [0, 1], linestyle="--", color="gray")
        plt.xlabel("False Positive Rate")
        plt.ylabel("True Positive Rate")
        plt.legend()
        plt.title("ROC Curve")
        # 直接打开图片
        # plt.show()
        # 保存图片
        # plt.savefig(os.path.join(self._featured_dir,"roc_importance.png"), dpi=300, bbox_inches="tight")
        # plt.close()
        # 直接打印
        print("\nROC Curve (first 5 points):")
        for i in range(min(5, len(fpr))):
            print(f"FPR={fpr[i]:.4f}, TPR={tpr[i]:.4f}")


if __name__ == "__main__":
    trainer = BinanceTrainer("BTCUSDT", "4h", "shape")
    # trainer.train_lightgbm()
