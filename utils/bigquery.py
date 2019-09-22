from google.cloud import bigquery
from google.cloud.exceptions import NotFound

def check_table_exist(table_id):
    client = bigquery.Client()
    try:
        table = client.get_table(table_id)
        if table:
            print('Table {}\'s existence sucessfully proved!'.format(table_id))
            return True
    except NotFound as error:
        # ...do some processing ...
        print('Whoops! Table {} doesn\'t exist here! Ref: {}'.format(table_id, error.response))
        return False

def create_dataset(dataset_id):
    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset(dataset_id)

    try:
        bigquery_client.get_dataset(dataset_ref)
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset = bigquery_client.create_dataset(dataset)
        print('Dataset {} created.'.format(dataset.dataset_id))

def exist_record(query):
    bigquery_client = bigquery.Client()

    try:
        query_job = bigquery_client.query(query)
        is_exist = len(list(query_job.result())) >= 1
        return is_exist
    except Exception as e:
        print(e)

    return False