from airflow import DAG
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from datetime import datetime, timedelta
from google.cloud import bigquery
from dags.webservices.operators.flt_bl import CheckFLT_BLOperator, InsertFLT_BLOperator
import requests
from utils.json import is_jsonable

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

def get_json_data(**context):
    run_date = context['yesterday_ds_nodash']
    URL = "http://10.223.19.8:1988/api/partner/aitsgetreportodsbyday?startday={fromDate}&endday={toDate}".format(fromDate=run_date, toDate=run_date)
    response = requests.get(url = URL).json()
    if is_jsonable(response):
        context['task_instance'].xcom_push(key='json_data', value=response)
        return True
    return False

def insert_data_to_table(**context):
    json_data = context['task_instance'].xcom_pull(task_ids='get_flt_bl', key='json_data')

    results = []
    for x in json_data:
        tmp_dict = {}
        tmp_dict['FlightDate'] = datetime.strptime(x['FlightDate'], '%Y%m%d').strftime('%Y-%m-%d')
        tmp_dict['CarrierCode'] = x['CarrierCode']
        tmp_dict['FlightNumber'] = x['FlightNumber'].strip()
        tmp_dict['DepartureDate'] = datetime.strptime(x['DepartureDate'], '%Y%m%d').strftime('%Y-%m-%d')
        tmp_dict['ArrivalDate'] = datetime.strptime(x['ArrivalDate'], '%Y%m%d').strftime('%Y-%m-%d')
        tmp_dict['Leg_STD'] = x['Leg_STD']
        tmp_dict['Leg_STA'] = x['Leg_STA']
        tmp_dict['ADT'] = x['ADT']
        tmp_dict['CHD'] = x['CHD']
        tmp_dict['INF'] = x['INF']
        tmp_dict['DepartureStation'] = x['DepartureStation']
        tmp_dict['ArrivalStation'] = x['ArrivalStation']
        tmp_dict['Segment'] = x['Segment']
        tmp_dict['Capacity'] = x['Capacity']
        tmp_dict['BD'] = x['BD']
        tmp_dict['NS'] = x['NS']
        tmp_dict['PaxRev'] = x['PaxRev']
        tmp_dict['BaggageAmount'] = float(x['BaggageAmount'])
        tmp_dict['Other_Rev'] = float(x['Other_Rev'])
        tmp_dict['CargoRev'] = float(x['CargoRev'])
        tmp_dict['VCost'] = float(x['VCost'])
        tmp_dict['FCost'] = float(x['FCost'])
        tmp_dict['TotalCost'] = float(x['TotalCost'])
        tmp_dict['QTQN'] = x['QTQN']
        tmp_dict['FLS_TYPE'] = x['FLS_TYPE']
        results.append(tmp_dict)

    client = bigquery.Client()
    table = client.get_table("{}.{}.{}".format(project_id,dataset_id,table_id))
    errors = client.insert_rows(table, results)

    if errors == []:
        print("Insert successfully!")
    else:
        print("Insert error.")

# run_this = PythonOperator(
#     task_id='query_stackoverflow',
#     provide_context=True,
#     python_callable=query_stackoverflow,
#     dag=dag,
# )

t1 = CheckFLT_BLOperator(
    task_id="check_table_flt_bl",
    table_id="{}.{}.{}".format(project_id,dataset_id,table_id),
    dag=dag,
)

t2 = ShortCircuitOperator(
    task_id="get_flt_bl",
    provide_context=True,
    python_callable=get_json_data,
    dag=dag,
)

t3 = PythonOperator(
    task_id="insert_table_flt_bl",
    provide_context=True,
    python_callable=insert_data_to_table,
    dag=dag,
)

# t3 = InsertFLT_BLOperator(
#     task_id="insert_table_flt_bl",
#     table_id="{}.{}.{}".format(project_id,dataset_id,table_id),
#     dag=dag,
# )

t1 >> t2 >> t3

