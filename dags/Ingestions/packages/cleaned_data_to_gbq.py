from pandas.io import gbq
from google.cloud import storage
from google.cloud.storage import blob
from io import StringIO
import pandas as pd
import os

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

def load_data_from_gcs_to_gbq():
    try:
        path_gcs = 'cleaned_data/users.csv'
        df = download_data_from_gcs(path_gcs)
        data = df[['created_at','updated_at','id','first_name','last_name']]
        #upload data to gbq
        name_table = 'exam_kittisak.users'
        upload_data_to_gbq(data,name_table)
        path_gcs = 'cleaned_data/user_log.csv'
        df = download_data_from_gcs(path_gcs)
        data = df[['created_at','updated_at','id','user_id','action','success']]
        #upload data to gbq
        name_table = 'exam_kittisak.user_log'
        upload_data_to_gbq(data,name_table)
    except Exception as e:
        raise e

def upload_data_to_gbq(data,name_table):
    try:
        gbq.to_gbq(data, name_table, project_id='de-exam-kittisak', if_exists='append')
        message = "status:200 Success send data to gbq"
        print(message)
        return message
    except Exception as e:
        raise e

        
def main_run_data_to_gbq():
    load_data_from_gcs_to_gbq()