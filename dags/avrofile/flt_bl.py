import os
from google.cloud import bigquery, storage
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from google.cloud.exceptions import NotFound
import requests
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
from utils.bigquery import check_table_exist
from utils.json import is_jsonable
from utils.datetime import day_from_unix_epoch

class FLT_BLTask(object):
    def __init__(self, dag):
        self.dag = dag
        self.table_id = 'dwh-demo.staging_flight.flt_bl'
        self.task_id_1 = 'check_table_flt_bl'
        self.task_id_2 = 'get_flt_bl'
        self.task_id_3 = 'transform_data'
        self.task_id_4 = 'create_avro_file'
        self.task_id_5 = 'upload_to_cloud_storage'

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

        t4 = PythonOperator(
            task_id=self.task_id_4,
            python_callable=self.__create_avro,
            provide_context=True,
            dag=self.dag,
        )

        t5 = PythonOperator(
            task_id=self.task_id_5,
            python_callable=self.__upload_cloud_storage,
            provide_context=True,
            dag=self.dag
        )

        t1 >> t2 >> t3 >> t4 >> t5
        return t1, t5
    
    def __check_table(self, **context):
        if not check_table_exist(self.table_id):
            schema = [
                bigquery.SchemaField("flight_date", "DATE", mode="NULLABLE"),
                bigquery.SchemaField("carrier_code", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("flight_number", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("departure_date", "DATE", mode="NULLABLE"),
                bigquery.SchemaField("arrival_date", "DATE", mode="NULLABLE"),
                bigquery.SchemaField("leg_std", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("leg_sta", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("adt", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("chd", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("inf", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("departure_station", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("arrival_station", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("segment", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("capacity", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("bd", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("ns", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("pax_rev", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("baggage_amount", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("other_rev", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("cargo_rev", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("v_cost", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("f_cost", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("total_cost", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("qtqn", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("fls_type", "STRING", mode="NULLABLE"),
            ]

            client = bigquery.Client()
            table = bigquery.Table(self.table_id, schema=schema)
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="flight_date"
            )
            table.clustering_fields = ["flight_number"]
            table = client.create_table(table)
            print(
                "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
            )

    def __get_json_data(self, **context):
        run_date = context['yesterday_ds_nodash']
        URL = "http://10.223.19.8:1988/api/partner/aitsgetreportodsbyday?startday={fromDate}&endday={toDate}".format(fromDate=run_date, toDate=run_date)
        response = requests.get(url = URL).json()
        print("asdasdasasasdasdasdasdsdfsfgfd, {}", run_date)
        print(day_from_unix_epoch(datetime.strptime(response[0]['FlightDate'], '%Y%m%d')))
        if is_jsonable(response):
            context['task_instance'].xcom_push(key='json_data', value=response)
            return True
        return False

    def __transform_data(self, **context):
        data = context['task_instance'].xcom_pull(task_ids=self.task_id_2, key='json_data')

        results = []
        for x in data:
            tmp_dict = {}
            tmp_dict['flight_date'] = day_from_unix_epoch(datetime.strptime(x['FlightDate'], '%Y%m%d') + timedelta(hours=7))
            tmp_dict['carrier_code'] = x['CarrierCode']
            tmp_dict['flight_number'] = x['FlightNumber'].strip()
            tmp_dict['departure_date'] = day_from_unix_epoch(datetime.strptime(x['DepartureDate'], '%Y%m%d') + timedelta(hours=7))
            tmp_dict['arrival_date'] = day_from_unix_epoch(datetime.strptime(x['ArrivalDate'], '%Y%m%d') + timedelta(hours=7))
            tmp_dict['leg_std'] = x['Leg_STD']
            tmp_dict['leg_sta'] = x['Leg_STA']
            tmp_dict['adt'] = x['ADT']
            tmp_dict['chd'] = x['CHD']
            tmp_dict['inf'] = x['INF']
            tmp_dict['departure_station'] = x['DepartureStation']
            tmp_dict['arrival_station'] = x['ArrivalStation']
            tmp_dict['segment'] = x['Segment']
            tmp_dict['capacity'] = x['Capacity']
            tmp_dict['bd'] = x['BD']
            tmp_dict['ns'] = x['NS']
            tmp_dict['pax_rev'] = x['PaxRev']
            tmp_dict['baggage_amount'] = float(x['BaggageAmount'])
            tmp_dict['other_rev'] = float(x['Other_Rev'])
            tmp_dict['cargo_rev'] = float(x['CargoRev'])
            tmp_dict['v_cost'] = float(x['VCost'])
            tmp_dict['f_cost'] = float(x['FCost'])
            tmp_dict['total_cost'] = float(x['TotalCost'])
            tmp_dict['qtqn'] = x['QTQN']
            tmp_dict['fls_type'] = x['FLS_TYPE']
            results.append(tmp_dict)
        
        context['task_instance'].xcom_push(key='transform_data', value=results)
        return results

    def __create_avro(self, **context):
        data = context['task_instance'].xcom_pull(task_ids=self.task_id_3, key='transform_data')
        schema = avro.schema.Parse(open("dags/avrofile/flt_bl.avsc", "rb").read())

        writer = DataFileWriter(open("dags/avrofile/flt_bl.avro", "wb"), DatumWriter(), schema, codec="deflate")
        for d in data:
            writer.append(d)
        writer.close()

    def __upload_cloud_storage(self, **context):
        client = storage.Client()
        try:
            bucket = client.get_bucket('jpa_staging_demo')
            blob = bucket.blob('flt_bl/flt_bl.avro')
            blob.upload_from_filename(filename='dags/avrofile/flt_bl.avro')
        except Exception as e:
            print(e)
        
        if os.path.exists("dags/avrofile/flt_bl.avro"):
            os.remove("dags/avrofile/flt_bl.avro")
