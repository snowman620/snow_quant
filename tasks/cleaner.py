#!/usr/bin/env python
# coding: utf-8
# author: snowman

from celery_app import app
from common.log_com import LogManager
from executor.cleaner import BinanceCsvCleaner

logger = LogManager(name="cleaner").get_logger()


@app.task(bind=True, name="task_check_binance_spot_monthly_klines_csv", queue="cleaner")
def task_check_binance_spot_monthly_klines_csv(self, **kwargs):
    try:
        cleaner = BinanceCsvCleaner("monthly", kwargs["symbol"], kwargs["interval"], kwargs["date"])
        cleaner.check_spot_klines_csv()
    except Exception as e:
        logger.error(f"Check Failed")
        raise


@app.task(bind=True, name="task_check_binance_spot_daily_klines_csv", queue="cleaner")
def task_check_binance_spot_daily_klines_csv(self, **kwargs):
    try:
        cleaner = BinanceCsvCleaner("daily", kwargs["symbol"], kwargs["interval"], kwargs["date"])
        cleaner.check_spot_klines_csv()
    except Exception as e:
        logger.error(f"Check Failed")
        raise
