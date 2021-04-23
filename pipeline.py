"""Configurations needed for the Pipeline.
"""
import os
import requests
import json
from datetime import timedelta
import pandas as pd

from typing import List
from io import BytesIO
from zipfile import ZipFile
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from prefect import Flow, task, context
from prefect.executors import LocalExecutor
from prefect.schedules import IntervalSchedule

SOURCE_URL = os.getenv('_AWS_S3_URL_SOURCE')
HOSTNAME = os.getenv('_PGSQL_HOSTNAME', '127.0.0.1')
PORT = os.getenv('_PGSQL_PORT', '5432')
USERNAME = os.getenv('_PGSQL_USER')
PASSWORD = os.getenv('_PGSQL_PASS')
DBNAME = os.getenv('_PGSQL_DATABASE')
CONNECTION_URI = f'postgresql+psycopg2://{USERNAME}:{PASSWORD}@{HOSTNAME}:{PORT}/{DBNAME}'  # noqa 501


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


@task
def load_orders(engine: Engine, orders: List[dict]) -> None:
    order_details = list()
    for order in orders:
        order_id = order.get('id')
        assert order_id, 'ORDER ID IS NULL!'

        details = {
            'id': order_id,
            'closed_at': order.get('closed_at'),
            'created_at': order.get('created_at'),
            'updated_at': order.get('updated_at'),
            'number': order.get('number'),
            'note': order.get('note'),
            'token': order.get('token'),
            'gateway': order.get('gateway'),
            'test': order.get('test'),
            'total_price': order.get('total_price'),
            'subtotal_price': order.get('subtotal_price'),
            'total_weight': order.get('total_weight'),
            'total_tax': order.get('total_tax'),
            'taxes_included': order.get('taxes_included'),
            'currency': order.get('currency'),
            'financial_status': order.get('financial_status'),
            'confirmed': order.get('confirmed'),
            'total_discounts': order.get('total_discounts'),
            'total_line_items_price': order.get('total_line_items_price'),
            'cart_token': order.get('cart_token'),
            'buyer_accepts_marketing': order.get('buyer_accepts_marketing'),
            'name': order.get('name'),
            'referring_site': order.get('referring_site'),
            'landing_site': order.get('landing_site'),
            'cancelled_at': order.get('cancelled_at'),
            'cancel_reason': order.get('cancel_reason'),
            'total_price_usd': order.get('total_price_usd'),
            'checkout_token': order.get('checkout_token'),
            'reference': order.get('reference'),
            'user_id': order.get('user_id'),
            'location_id': order.get('location_id'),
            'source_identifier': order.get('source_identifier'),
            'source_url': order.get('source_url'),
            'processed_at': order.get('processed_at'),
            'device_id': order.get('device_id'),
            'app_id': order.get('app_id'),
            'browser_ip': order.get('browser_ip'),
            'landing_site_ref': order.get('landing_site_ref'),
            'order_number': order.get('order_number'),
            'processing_method': order.get('processing_method'),
            'checkout_id': order.get('checkout_id'),
            'source_name': order.get('source_name'),
            'fulfillment_status': order.get('fulfillment_status'),
            'tags': order.get('tags'),
            'contact_email': order.get('contact_email'),
            'order_status_url': order.get('order_status_url'),
        }
        order_details.append(details)

    orders_df = pd.DataFrame(order_details)
    orders_df.drop_duplicates(inplace=True)
    orders_df.to_sql('orders',
                     con=engine,
                     index=False,
                     if_exists='replace')


def build_pipeline() -> Flow:
    """Build Prefect Flow and return runnable pipeline.
    """
    engine = create_engine(CONNECTION_URI)

    schedule = IntervalSchedule(interval=timedelta(days=1))  # run daily
    with Flow('OrderPipeline', schedule=schedule) as flow:
        raw_data = download_source(SOURCE_URL)
        load_users(engine, raw_data)
        load_orders(engine, raw_data)

    return flow


if __name__ == '__main__':
    # Run pipeline with local executor
    flow = build_pipeline()
    flow.run(executor=LocalExecutor(), run_on_schedule=False)
