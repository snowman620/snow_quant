#!/usr/bin/env python
# coding: utf-8
# author: snowman

from pydantic_settings import BaseSettings, SettingsConfigDict


class EnvManager(BaseSettings):
    """环境变量管理"""
    APP_NAME: str = "snow_quant"
    REDIS_HOST: str = "127.0.0.1"
    REDIS_PORT: int = 6379
    RABBITMQ_HOST: str = "127.0.0.1"
    RABBITMQ_PORT: int = 5672
    RABBITMQ_USER: str = "guest"
    MYSQL_HOST: str = "127.0.0.1"
    MYSQL_PORT: int = 3306
    MYSQL_USER: str = "root"
    MYSQL_PASS: str = "123456"
    DATA_PATH: str = "../.data"
    LOG_PATH: str = "./logs"
    SYMBOLS: list = ["BTCUSDT",]
    INTERVALS: list = ["4h", "2h", "1h",]
    
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

env_mgr = EnvManager()
