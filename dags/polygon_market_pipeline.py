import os
from datetime import datetime, timedelta
import requests

import sqlalchemy as sa
from pg_bulk_ingest import ingest

from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

import logging
_logger = logging.getLogger('pg_bulk_ingest')
_logger = _logger.setLevel('WARNING')

# 1. Define the function that makes up the single task of this DAG

@task(
    retries=1,
    retry_exponential_backoff=True,
    retry_delay=timedelta(seconds=10),
    max_retry_delay=timedelta(minutes=1),
)
def sync(
    dag_id,
    schema,
    table_name,
    high_watermark,
    delete,
):
    engine = sa.create_engine(
        url=os.environ['AIRFLOW__DATABASE__SQL_ALCHEMY_CONN'],
        future=True
    )

    if engine.url == os.environ['AIRFLOW__DATABASE__SQL_ALCHEMY_CONN']:
        _logger.warning('''
            You are ingesting into Airflow\'s metadata database.
            You should only do this for the pg_bulk_ingest test case.
            Change the engine url to match your production database.
        ''')

    # The SQLAlchemy definition of the table to ingest data into
    metadata = sa.MetaData()
    table = sa.Table(
        table_name,
        metadata,
        sa.Column("code", sa.VARCHAR(10), primary_key=True),
        sa.Column("suffix", sa.VARCHAR(2), primary_key=True),
        sa.Column("description", sa.VARCHAR(), nullable=False),
        sa.Index(None, "code"),
        schema=schema,
    )

    def batches(high_watermark_value):
        # No data

        # Instantiate a list of tickers that will be pulled and looped over
        stock_ticker = "AMZN"
        # Set variables
        polygon_api_key = "NbEY6gQ8AXzdOOVEOcIj9hiO0Ifi99eh"
        context = get_current_context()
        ds = context.get("ds")
        # set date so that we get some actual data for testing
        ds = "2024-12-10"
        # Create the URL
        url = f"https://api.polygon.io/v1/open-close/{stock_ticker}/{ds}?adjusted=true&apiKey={polygon_api_key}"
        print(url)
        response = requests.get(url)
        # Return the raw data
        print(response.json())

        yield from ()

    def on_before_visible(
        conn, ingest_table, source_modified_date
    ):
        pass

    with engine.connect() as conn:
        ingest(
             conn,
             metadata,
             batches,
             on_before_visible=on_before_visible,
             high_watermark=high_watermark,
             delete=delete,
        )


# 2. Define the function that creates the DAG from the single task

def create_dag(dag_id, schema, table_name):
    @dag(
        dag_id=dag_id,
        start_date=datetime(2022, 5, 16),
        schedule='@daily',
        catchup=False,
        max_active_runs=1,
        params={
            'high_watermark': '__LATEST__',
            'delete': '__BEFORE_FIRST_BATCH__',
        },
    )

    def Pipeline():
        sync.override(
            outlets=[
                Dataset(f'postgresql://datasets//{schema}/{table_name}'),
            ]
        )(
            dag_id=dag_id,
            schema=schema,
            table_name=table_name,
            high_watermark="",
            delete="",
        )
    Pipeline()

# 3. Call the function that creates the DAG

create_dag('polygon_market_stocks', 'dbt', 'polygon_market_stocks')
