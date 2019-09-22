from google.cloud import bigquery
from datetime import datetime
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from google.cloud.exceptions import NotFound
import requests
from utils.bigquery import check_table_exist
from utils.json import is_jsonable

class FLT_BLTask(object):
    def __init__(self, dag):
        self.dag = dag
        self.table_id = 'dwh-demo.staging_flight.flt_bl'
        self.task_id_1 = 'check_table_flt_bl'
        self.task_id_2 = 'get_flt_bl'
        self.task_id_3 = 'transform_data'
        self.task_id_4 = 'delete_data'
        self.task_id_5 = 'insert_data_to_bigquery'

    def tasks(self):
        t1 = PythonOperator(
            task_id=self.task_id_1,
            python_callable=self.__check_table,
            provide_context=True,
            dag=self.dag
        )

        t2 = ShortCircuitOperator(
            task_id=self.task_id_2,
            python_callable=self.__get_json_data,
            provide_context=True,
            dag=self.dag,
        )

        t3 = PythonOperator(
            task_id=self.task_id_3,
            python_callable=self.__transform_data,
            provide_context=True,
            dag=self.dag
        )

        t4 = ShortCircuitOperator(
            task_id=self.task_id_4,
            python_callable=self.__delete_data,
            provide_context=True,
            dag=self.dag,
        )

        t5 = PythonOperator(
            task_id=self.task_id_5,
            python_callable=self.__insert_data,
            provide_context=True,
            dag=self.dag
        )

        t1 >> t2 >> t3 >> t4 >> t5
        return t1, t5
    
    def __check_table(self, **context):
        if not check_table_exist(self.table_id):
            schema = [
                bigquery.SchemaField("FlightDate", "DATE", mode="NULLABLE"),
                bigquery.SchemaField("CarrierCode", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("FlightNumber", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("DepartureDate", "DATE", mode="NULLABLE"),
                bigquery.SchemaField("ArrivalDate", "DATE", mode="NULLABLE"),
                bigquery.SchemaField("Leg_STD", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("Leg_STA", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("ADT", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("CHD", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("INF", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("DepartureStation", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("ArrivalStation", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("Segment", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("Capacity", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("BD", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("NS", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("PaxRev", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("BaggageAmount", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("Other_Rev", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("CargoRev", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("VCost", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("FCost", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("TotalCost", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("QTQN", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("FLS_TYPE", "STRING", mode="NULLABLE"),
            ]

            client = bigquery.Client()
            table = bigquery.Table(self.table_id, schema=schema)
            table = client.create_table(table)
            print(
                "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
            )

    def __get_json_data(self, **context):
        run_date = context['yesterday_ds_nodash']
        URL = "http://10.223.19.8:1988/api/partner/aitsgetreportodsbyday?startday={fromDate}&endday={toDate}".format(fromDate=run_date, toDate=run_date)
        response = requests.get(url = URL).json()
        if is_jsonable(response):
            context['task_instance'].xcom_push(key='json_data', value=response)
            return True
        return False

    def __transform_data(self, **context):
        data = context['task_instance'].xcom_pull(task_ids=self.task_id_2, key='json_data')

        results = []
        for x in data:
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
        
        context['task_instance'].xcom_push(key='transform_data', value=results)
        return results

    def __delete_data(self, **context):
        client = bigquery.Client()
        query = """
            #standardSQL
            DELETE {table_id} WHERE FlightDate = {date}
        """.format(table_id=self.table_id, date=context['yesterday_ds_nodash'])

        try:
            query_job = client.query(query)
            query_job.result()
            return True
        except Exception as e:
            print(e)

        return False

    def __insert_data(self, **context):
        data = context['task_instance'].xcom_pull(task_ids=self.task_id_3, key='transform_data')

        client = bigquery.Client()
        table = client.get_table(self.table_id)
        errors = client.insert_rows(table, data)

        if errors == []:
            print("Insert successfully!")
        else:
            print("Insert error.")
