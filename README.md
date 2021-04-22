# Order Data ETL with Prefect
Setup a daily pipeline to ingest raw Order API data into a PostgreSQL db.

## TODOs:
1. Define schema with SQLAlchemy
   + Pandas inference isn't great
2. Load order line items
3. Define order summary view
4. Set Prefect Scheduler

## Table of Contents
- [Order Data ETL with Prefect](#order-data-etl-with-prefect)
  - [TODOs:](#todos)
  - [Table of Contents](#table-of-contents)
  - [Setup](#setup)
    - [Python Environment](#python-environment)
    - [Configuration](#configuration)
    - [Usage](#usage)
  - [Design](#design)
  - [Improvements](#improvements)

## Setup
Developed using Python >=3.7 for latest features like typing and dataclasses.

### Python Environment
+ Recommend using an isolated environment manager like `virtualenv`, `pyenv`, `pipenv`, etc.
  + Install dependencies: `pip install -r requirements.txt`

### Configuration
+ To avoid hardcoding resources, the pipeline will pull the following from environment variables:
  + `_PGSQL_HOSTNAME` - Name of the db host to connect
  + `_PGSQL_PORT` - Port number on the host
  + `_PGSQL_USER` - Username for auth
  + `_PGSQL_PASS` - Password for auth
  + `_PGSQL_DATABASE` - Default database name to use
  + `_AWS_S3_URL_SOURCE` - s3 url to source data (object URI not given)

Note, while not necessary, I use `direnv` for local development to automatically export these env variables based on my current working directory. It uses a dotfile called `.envrc` which is git ignored to safely store sensitive info like credentials locally.

In cloud environments, I tend to use services like Secrets Manager or HashiCorp Vault to fetch credentials at runtime or rely on default compute metadata for authentication within the cloud environment. The trade-off is that it makes the code less portable and introduces vendor lock-in.

### Usage
Run using a LocalExecutor with `python pipeline.py`

## Design
Decided to use Prefect for a lightweight, Pythonic ETL tool that is both easy to run+test locally as well as deploy to a production compute environment.
+ Only using the Python package for this demo, otherwise I would use Docker to run a Prefect Server to provide pgsql metadata storage, GraphQL API, web server, etc.


## Improvements
+ Load tasks should ideally be concurrent
+ Append-only pipeline and index order tables by `run_date`
  + Keep historical record of orders
  + Would need to upsert users
+ Better database normalization
  + Namely, split orders into fact and dimensions
  + Might be some other entities that could be factored out like location and device/app
+ Current implementation won't scale to larger datasets:
  + Ideally can hit API directly async and store intermediate results in S3
    + Avoids iterating over every file and every row within each file
  + Larger data volume would benefit from parallelism i.e. Dask instead of Pandas
    + Currently relies on loading data dump into memory entirely
      + If able to use blob storage, can reduce memory footprint of workers
