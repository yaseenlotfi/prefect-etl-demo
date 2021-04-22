"""Configurations needed for the Pipeline.
"""
import os
import requests
import json

from typing import List
from io import BytesIO
from zipfile import ZipFile
from prefect import Flow, task, context
from prefect.executors import LocalExecutor

SOURCE_URL = os.getenv('_AWS_S3_URL_SOURCE')
HOSTNAME = os.getenv('_PGSQL_HOSTNAME', '127.0.0.1')
PORT = os.getenv('_PGSQL_PORT', '5432')
USERNAME = os.getenv('_PGSQL_USER')
PASSWORD = os.getenv('_PGSQL_PASS')
DBNAME = os.getenv('_PGSQL_DATABASE')

user_insert_template = """
insert into users (user_id, email, phone, customer_locale)
values (%s, %s, %s, %s, %s)
"""


@task
def download_source(url: str) -> List[dict]:
    """Download the source data in-memory.
    """
    task_logger = context['logger']
    assert url, 'URL NOT FOUND!'

    # Download zip file
    res = requests.get(url)
    if res.status_code != 200:
        task_logger.error(f'FAILED TO DOWNLOAD DATA FROM {url}')
    data_bytes = BytesIO(res.content)
    file_dump = ZipFile(data_bytes)

    # Unpack contents into all orders
    orders = list()
    for filename in file_dump.namelist():
        for batch in json.loads(file_dump.read(filename)):
            batch_orders = batch.get(orders, [])
            if len(orders) == 0:
                continue
            orders.append(batch_orders)
    return orders


@task
def process_data(raw_orders: List[dict]):
    """Extract entities out of raw JSON dump.
    """
    pass


def build_pipeline() -> Flow:
    """Build Prefect Flow and return runnable pipeline.
    """
    with Flow('OrderPipeline') as flow:
        raw_data = download_source(SOURCE_URL)
        process_data(raw_data)

    return flow


if __name__ == '__main__':
    # Run pipeline with local executor
    flow = build_pipeline()
    flow.run(executor=LocalExecutor(), run_on_schedule=False)
