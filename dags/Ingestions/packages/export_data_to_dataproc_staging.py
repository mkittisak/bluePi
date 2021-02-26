from google.cloud import bigquery
import pandas as pd
import os
from google.cloud import storage
from google.cloud.storage import blob
import os
from io import StringIO

# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'de-exam-kittisak-c992d82a0e4a.json'

def qurey_gbq(table):
    try:
        query = 'SELECT * FROM `de-exam-kittisak.exam_kittisak.'+table+'`'
        df = pd.read_gbq(query,
                    project_id = "de-exam-kittisak",
                    dialect = 'standard')
        return(df)
    except Exception as error :
        raise error

def get_data_gbq():
    table = 'users'
    df_users = qurey_gbq(table)
    table = 'user_log'
    df_user_log = qurey_gbq(table)
    return df_users, df_user_log

def upload_data_to_gcs(df,path_gcs):
    try:
        client = storage.Client()
        bucket = client.get_bucket('dataproc-staging-asia-southeast1-936648921070-vanb2y9d')
        bucket.blob(path_gcs).upload_from_string(df.to_csv(), 'text/csv')
        message = "success upload file to gcs"
        print(message)
        return message
    except Exception as e:
        message = "can not upload file to gcs: {}".format(e)
        print(message)
        raise e

def run_upload_data_to_dataproc_staging_gcs(df_users, df_user_log):
    path_gcs = 'data/users.csv'
    upload_data_to_gcs(df_users,path_gcs)
    path_gcs = 'data/user_log.csv'
    upload_data_to_gcs(df_user_log,path_gcs)

def main_run_data_to_dataproc_staging():
    df_users, df_user_log = get_data_gbq()
    run_upload_data_to_dataproc_staging_gcs(df_users, df_user_log)