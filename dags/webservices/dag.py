from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from google.cloud import bigquery
from dags.webservices.flt_bl import FLT_BLTask

project_id = 'dwh-demo'
dataset_id = 'staging_flight'
table_id = 'flt_bl'

default_args = {
    "owner": "kien tran",
    "depends_on_past": False,
    "start_date": datetime(2019, 9, 19),
    "email": ["kientd.aits@vietnamairlines.com"],
    "email_on_failure": ["kientd.aits@vietnamairlines.com"],
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2019, 1, 1),
}

dag = DAG("webservice", default_args=default_args, schedule_interval=timedelta(days = 1))

def query_stackoverflow(**context):
    client = bigquery.Client()
    query_job = client.query("""
        SELECT
          CONCAT(
            'https://stackoverflow.com/questions/',
            CAST(id as STRING)) as url,
          view_count
        FROM `bigquery-public-data.stackoverflow.posts_questions`
        WHERE tags like '%google-bigquery%'
        ORDER BY view_count DESC
        LIMIT 10""")

    results = query_job.result()  # Waits for job to complete.

    for row in results:
        print("{} : {} views".format(row.url, row.view_count))
    return results.total_rows

with dag:
    start = DummyOperator(
        task_id='start'
    )

    end = DummyOperator(
        task_id='end'
    )

    flt_blTask = FLT_BLTask(dag).tasks()
    start >> flt_blTask[0]
    flt_blTask[1] >> end