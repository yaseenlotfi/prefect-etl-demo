# Order Data ETL with Prefect
Setup a daily pipeline to ingest raw Order API data into a PostgreSQL db.

## Table of Contents
- [Order Data ETL with Prefect](#order-data-etl-with-prefect)
  - [Table of Contents](#table-of-contents)
  - [Setup](#setup)
    - [Python Environment](#python-environment)
    - [Configuration](#configuration)
  - [Design](#design)

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

## Design
Decided to use Prefect for a lightweight, Pythonic ETL tool that is both easy to run+test locally as well as deploy to a production compute environment.
+ Only using the Python package for this demo, otherwise I would use Docker to run a Prefect Server to provide pgsql metadata storage, GraphQL API, web server, etc.
