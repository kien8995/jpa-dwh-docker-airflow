from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from google.cloud import bigquery
import requests
from datetime import datetime
from utils.bigquery import check_table_exist

class CheckFLT_BLOperator(BaseOperator):

    @apply_defaults
    def __init__(self, table_id, *args, **kwargs):
        self.table_id = table_id
        super(CheckFLT_BLOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        if not check_table_exist(self.table_id):
            self.create_table_flt_bl(self.table_id)
    
    def create_table_flt_bl(self, table_id):
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
        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table)
        print(
            "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
        )

class InsertFLT_BLOperator(BaseOperator):

    @apply_defaults
    def __init__(self, table_id, *args, **kwargs):
        self.table_id = table_id
        super(InsertFLT_BLOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        json_data = context['task_instance'].xcom_pull(task_ids='get_flt_bl', key='json_data')
        transformed_data = self.transform_data(json_data)
        self.insert_data_to_table(self.table_id, transformed_data)

    def insert_data_to_table(self, table_id, data):
        client = bigquery.Client()
        table = client.get_table(table_id)
        errors = client.insert_rows(table, data)

        if errors == []:
            print("Insert successfully!")
        else:
            print("Insert error.")

    def transform_data(self, data):
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
        
        # print(results)
        return results
