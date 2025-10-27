#!/usr/bin/env python
# coding: utf-8
# author: snowman

import logging
import json
import sys
import threading
from datetime import datetime


class LogManager:
    """日志管理器"""

    _instances = {}
    _lock = threading.Lock()

    def __new__(cls, name="logcom"):
        with cls._lock:
            if name not in cls._instances:
                instance = super().__new__(cls)
                cls._instances[name] = instance
        return cls._instances[name]

    def __init__(self, name="logcom"):
        if hasattr(self, "_initialized") and self._initialized:
            return

        self._logger = logging.getLogger(name)
        self._logger.setLevel(logging.INFO)

        if not self._logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            handler.setFormatter(JSONFormatter())
            self._logger.addHandler(handler)

        self._logger.propagate = False
        self._initialized = True

    def get_logger(self):
        return self._logger


class JSONFormatter(logging.Formatter):
    """JSON格式"""

    def format(self, record):
        log_record = {
            "created": datetime.fromtimestamp(record.created).strftime("%Y-%m-%d %H:%M:%S"),
            "level": record.levelname,
            "module": record.name,
            "filename": record.filename,
            "lineno": record.lineno,
            "message": record.getMessage(),
        }
        return json.dumps(log_record, ensure_ascii=False)
