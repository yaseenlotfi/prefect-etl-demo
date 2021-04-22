"""Configurations needed for the Pipeline.
"""
import os
import requests
import json
import pandas as pd

from typing import List
from io import BytesIO
from zipfile import ZipFile
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from prefect import Flow, task, context
from prefect.executors import LocalExecutor

SOURCE_URL = os.getenv('_AWS_S3_URL_SOURCE')
HOSTNAME = os.getenv('_PGSQL_HOSTNAME', '127.0.0.1')
PORT = os.getenv('_PGSQL_PORT', '5432')
USERNAME = os.getenv('_PGSQL_USER')
PASSWORD = os.getenv('_PGSQL_PASS')
DBNAME = os.getenv('_PGSQL_DATABASE')
CONNECTION_URI = f'postgresql+psycopg2://{USERNAME}:{PASSWORD}@{HOSTNAME}:{PORT}/{DBNAME}'  # noqa 501

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
    all_orders = list()
    for filename in file_dump.namelist():
        batch = json.loads(file_dump.read(filename))
        orders = batch.get('orders', [])
        if len(orders) == 0:
            continue
        for order in orders:
            all_orders.append(order)
    return all_orders


@task
def load_users(engine: Engine, orders: List[dict]) -> None:
    users = list()
    for order in orders:
        user_id = order.get('user_id')
        assert user_id, 'USER ID IS NULL!'

        user = {
            'id': user_id,
            'email': order.get('email'),
            'phone': order.get('phone'),
            'customer_locale': order.get('customer_locale'),
        }
        users.append(user)

    user_df = pd.DataFrame(users)
    user_df.drop_duplicates(inplace=True)
    user_df.to_sql('users',
                   con=engine,
                   index=False,
                   if_exists='replace')


def build_pipeline() -> Flow:
    """Build Prefect Flow and return runnable pipeline.
    """
    engine = create_engine(CONNECTION_URI)

    with Flow('OrderPipeline') as flow:
        raw_data = download_source(SOURCE_URL)
        load_users(engine, raw_data)

    return flow


if __name__ == '__main__':
    # Run pipeline with local executor
    flow = build_pipeline()
    flow.run(executor=LocalExecutor(), run_on_schedule=False)
