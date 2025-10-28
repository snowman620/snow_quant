#!/usr/bin/env python
# coding: utf-8
# author: snowman

import os
import time
import hashlib
import requests
import zipfile
from common.env_com import env_mgr
from common.log_com import LogManager

logger = LogManager(name="collector").get_logger()


class BinanceCsvDownloader:
    """币安CSV文件下载器"""
    
    def __init__(self, period, symbol, interval, date):
        self._base_url = "https://data.binance.vision/data"
        self._base_dir = env_mgr.DATA_PATH
        self._period = period
        self._symbol = symbol
        self._interval = interval
        self._date = date

    def _download_file(self, url, filepath, timeout = 10, max_retries = 3, delay = 2):
        for n in range(1, max_retries + 1):
            try:
                resp = requests.get(url, timeout=timeout)
                resp.raise_for_status()
                with open(filepath, "wb") as f:
                    f.write(resp.content)
                return True
            except (requests.RequestException, Exception) as e:
                if n < max_retries:
                    time.sleep(delay)
                else:
                    logger.error(f"Download Failed: {url}")
                    raise e

    def _sha256sum(self, filepath, chunk_size=8192):
        """计算文件sha256值"""
        sha256 = hashlib.sha256()
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(chunk_size), b""):
                sha256.update(chunk)
        return sha256.hexdigest()

    def _read_checksum(self, filepath):
        """解析CHECKSUM文件"""
        with open(filepath, "r", encoding="utf-8") as f:
            line = f.readline().strip()
        return line.split()[0]

    def _save_csv(self):
        """把ZIP包解压成CSV文件"""
        filename = f"{self._symbol}-{self._interval}-{self._date}"
        path_zip = os.path.join(self._base_dir, "binance", "raw", "spot", self._period, "klines", self._symbol, self._interval, f"{filename}.zip")
        extract_dir = os.path.join(self._base_dir, "binance", "csv", "spot", self._period, "klines", self._symbol, self._interval)
        os.makedirs(extract_dir, exist_ok=True)
        path_csv = os.path.join(extract_dir, f"{filename}.csv")
        if not os.path.exists(path_csv):
            with zipfile.ZipFile(path_zip, 'r') as zf:
                zf.extractall(extract_dir)

    def download_spot_klines_csv(self):
        """下载币安现货K线CSV文件"""
        filename = f"{self._symbol}-{self._interval}-{self._date}.zip"
        checksum = f"{filename}.CHECKSUM"
        url_filename = f"{self._base_url}/spot/{self._period}/klines/{self._symbol}/{self._interval}/{filename}"
        url_checksum = f"{self._base_url}/spot/{self._period}/klines/{self._symbol}/{self._interval}/{checksum}"
        path_dir = os.path.join(self._base_dir, "binance", "raw", "spot", self._period, "klines", self._symbol, self._interval)
        path_filename = os.path.join(path_dir, filename)
        path_checksum = os.path.join(path_dir, checksum)
        os.makedirs(path_dir, exist_ok=True)
        if not os.path.exists(path_filename):
            self._download_file(url_filename, path_filename)
        if not os.path.exists(path_checksum):
            self._download_file(url_checksum, path_checksum)
        md5_real = self._sha256sum(path_filename)
        md5_check = self._read_checksum(path_checksum)
        if md5_real.lower() != md5_check.lower():
            os.remove(path_filename)
            os.remove(path_checksum)
            raise ValueError("MD5 Check Failed")
        self._save_csv()


class BinanceRealTimeDownloader:
    """币安实时数据下载器"""
    pass