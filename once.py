#!/usr/bin/env python
# coding: utf-8
# author: snowman

from celery_app import app
from datetime import datetime, timedelta


SYMBOLS = ["ETHUSDT",]
INTERVALS = ["5m",]
MONTHS = ["2025-10",]
DAYS = ("2025-11-01", "2025-11-01")


def download_binance_spot_monthly_klines_csv():
    """手动执行，按月下载币安现货K线历史数据"""
    for symbol in SYMBOLS:
        for interval in INTERVALS:
            for month in MONTHS:
                app.send_task(
                    "task_download_binance_spot_monthly_klines_csv", 
                    kwargs={"symbol": symbol, "interval": interval, "date": month},
                    queue="collector"
                )


def download_binance_spot_daily_klines_csv():
    """手动执行，按天下载当前月币安现货K线历史数据"""
    start = datetime.strptime(DAYS[0], "%Y-%m-%d").date()
    end = datetime.strptime(DAYS[1], "%Y-%m-%d").date()
    for symbol in SYMBOLS:
        for interval in INTERVALS:
            day = start
            while day <= end:
                app.send_task(
                    "task_download_binance_spot_daily_klines_csv", 
                    kwargs={"symbol": symbol, "interval": interval, "date": str(day)},
                    queue="collector"
                )
                day += timedelta(days=1)


def check_binance_spot_monthly_klines_csv():
    """手动执行，按月校验币安现货K线历史数据"""
    for symbol in SYMBOLS:
        for interval in INTERVALS:
            app.send_task(
                "task_check_binance_spot_monthly_klines_csv", 
                kwargs={"symbol": symbol, "interval": interval},
                queue="cleaner"
            )


def check_binance_spot_daily_klines_csv():
    """手动执行，按天校验币安现货K线历史数据"""
    for symbol in SYMBOLS:
        for interval in INTERVALS:
            app.send_task(
                "task_check_binance_spot_daily_klines_csv", 
                kwargs={"symbol": symbol, "interval": interval},
                queue="cleaner"
            )


if __name__ == "__main__":
    download_binance_spot_monthly_klines_csv()
    # download_binance_spot_daily_klines_csv()
    # check_binance_spot_monthly_klines_csv()
    # check_binance_spot_daily_klines_csv()
    pass
