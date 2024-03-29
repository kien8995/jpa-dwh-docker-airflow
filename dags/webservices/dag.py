from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from dags.webservices.flt_bl import FLT_BLTask

default_args = {
    "owner": "kien tran",
    "depends_on_past": False,
    "start_date": datetime(2019, 9, 19),
    "email": ["kientd.aits@vietnamairlines.com"],
    "email_on_failure": ["kientd.aits@vietnamairlines.com"],
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2019, 1, 1),
}

dag = DAG("webservice", default_args=default_args, schedule_interval=timedelta(days = 1))

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