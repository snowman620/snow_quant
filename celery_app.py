#!/usr/bin/env python
# coding: utf-8
# author: snowman

from celery import Celery
from common.env_com import env_mgr

app = Celery("snow_quant", broker=f"pyamqp://{env_mgr.RABBITMQ_USER}@{env_mgr.RABBITMQ_HOST}//")

app.conf.update(
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,
)

app.conf.task_queues = {
    "collector": {
        "exchange": "celery",
        "routing_key": "collector",
    },
    "cleaner": {
        "exchange": "celery",
        "routing_key": "cleaner",
    },
}

app.autodiscover_tasks(["tasks.collector", "tasks.cleaner"])
