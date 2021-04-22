"""Configurations needed for the Pipeline.
"""
import os

from typing import List
from prefect import Flow, task, context
from prefect.executors import LocalExecutor

SOURCE_URL = os.getenv('_AWS_S3_URL_SOURCE')
HOSTNAME = os.getenv('_PGSQL_HOSTNAME', '127.0.0.1')
PORT = os.getenv('_PGSQL_PORT', '5432')
USERNAME = os.getenv('_PGSQL_USER')
PASSWORD = os.getenv('_PGSQL_PASS')
DBNAME = os.getenv('_PGSQL_DATABASE')


@task
def download_source(url: str) -> List:
    """Download the source data locally.
    """
    task_logger = context['logger']
    assert url, 'URL NOT FOUND!'

    task_logger.info(f'Fetching data from {url}')
    return list()


def build_pipeline() -> Flow:
    """Build Prefect Flow and return runnable pipeline.
    """
    with Flow('OrderPipeline') as flow:
        raw_data = download_source(SOURCE_URL)

    return flow


if __name__ == '__main__':
    # Run pipeline with local executor
    flow = build_pipeline()
    flow.run(executor=LocalExecutor())
