import pandas as pd
import csv
from google.cloud import storage
from google.cloud.storage import blob
import os
from io import StringIO

# os.path.abspath(os.getcwd())
# print(os.getcwd())
# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'de-exam-kittisak-c992d82a0e4a.json'

def download_data_from_gcs(path_gcs):
    client = storage.Client()
    bucket = client.get_bucket('bluepi-lake')
    blob = bucket.blob(path_gcs)
    #Download contents
    data = blob.download_as_string()
    data = str(data,'utf-8')
    data = StringIO(data) 
    df = pd.read_csv(data)
    return df

def load_csv_from_gcs():
    try:
        path_gcs = 'raw_data/users.csv'
        df = download_data_from_gcs(path_gcs)
        df_users = df[['created_at','updated_at','id','first_name','last_name']]
        path_gcs = 'raw_data/user_log.csv'
        df = download_data_from_gcs(path_gcs)
        df_user_log = df[['created_at','updated_at','id','user_id','action','status']]
        return df_users, df_user_log
    except Exception as e:
        raise e

def transform_data(df_user_log):
    df_user_log.columns = df_user_log.columns.str.replace('status','success')
    df_user_log['success'] = df_user_log['success'].astype('bool')
    return df_user_log

def run_upload_data_to_gcs(df_users, df_user_log):
    path_gcs = 'cleaned_data/users.csv'
    upload_data_to_cleaned_gcs(df_users,path_gcs)
    path_gcs = 'cleaned_data/user_log.csv'
    upload_data_to_cleaned_gcs(df_user_log,path_gcs)

def upload_data_to_cleaned_gcs(df,path_gcs):
    try:
        client = storage.Client()
        bucket = client.get_bucket('bluepi-lake')
        bucket.blob(path_gcs).upload_from_string(df.to_csv(), 'text/csv')
        message = "success upload file to gcs"
        print(message)
        return message
    except Exception as e:
        message = "can not upload file to gcs: {}".format(e)
        print(message)
        raise e

def main_run_transform_data_to_cleaned_gcs():
    df_users, df_user_log = load_csv_from_gcs()
    transform_data(df_user_log)
    run_upload_data_to_gcs(df_users, df_user_log)
