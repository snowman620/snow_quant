#!/usr/bin/env python
# coding: utf-8
# author: snowman

from celery_app import app
from common.log_com import LogManager
from executor.collector import BinanceCsvDownloader
from executor.cleaner import BinanceCsvCleaner

logger = LogManager(name="executor").get_logger()


@app.task(bind=True, name="task_download_binance_spot_monthly_klines_csv", queue="collector")
def task_download_binance_spot_monthly_klines_csv(self, **kwargs):
    """按月下载币安现货K线CSV文件,失败立即重试3次"""
    try:
        downloader = BinanceCsvDownloader("monthly", kwargs["symbol"], kwargs["interval"], kwargs["date"])
        downloader.download_spot_klines_csv()
    except Exception as e:
        if self.request.retries < self.max_retries:
            raise self.retry(exc=e, countdown=3)
        else:
            logger.error(f"Download Failed")
            raise


@app.task(bind=True, name="task_download_binance_spot_daily_klines_csv", queue="collector")
def task_download_binance_spot_daily_klines_csv(self, **kwargs):
    """按天下载币安现货K线CSV文件,失败立即重试3次"""
    try:
        downloader = BinanceCsvDownloader("daily", kwargs["symbol"], kwargs["interval"], kwargs["date"])
        downloader.download_spot_klines_csv()
    except Exception as e:
        if self.request.retries < self.max_retries:
            raise self.retry(exc=e, countdown=3)
        else:
            logger.error(f"Download Failed")
            raise


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
