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