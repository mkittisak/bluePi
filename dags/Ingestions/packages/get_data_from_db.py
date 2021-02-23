import psycopg2
import pandas as pd
import csv
from google.cloud import storage
from google.cloud.storage import blob
import os
from configparser import ConfigParser
import configparser

config = ConfigParser()
config.read('/home/airflow/gcs/dags/Ingestions/packages/config.ini')
cf = configparser,ConfigParser()

os.path.abspath(os.getcwd())
print(os.getcwd())


def connect_db():
    try:
        userconfig = (config.get("database", "user" ))
        pwconfig = (config.get("database", "password" ))
        hostconfig = (config.get("database", "host" ))
        portconfig = (config.get("database", "port" ))
        DBconfig = (config.get("database", "database" ))

        connection = psycopg2.connect(user = userconfig ,
                                        password = pwconfig ,
                                        host = hostconfig ,
                                        port = portconfig ,
                                        database = DBconfig )
        cursor = connection.cursor()
        return cursor
    except (Exception, psycopg2.Error) as error :
        print ("Error while fetching data from PostgreSQL", error)
        raise error

def get_data_db_users(cursor):
    try:
        postgreSQL_select_Query = "select * from users"
        cursor.execute(postgreSQL_select_Query)
        data_table_users = cursor.fetchall() 
        df = pd.DataFrame(data_table_users,columns=['created_at','updated_at','id','first_name','last_name'])
        path_gcs = 'raw_data/users.csv'
        message = upload_data_to_raw_data_gcs(df ,path_gcs)
        return message
    except Exception as error :
        print ("Error while fetching data from PostgreSQL", error)
        raise error

def get_data_db_user_log(cursor):
    try:
        postgreSQL_select_Query = "select * from user_log"
        cursor.execute(postgreSQL_select_Query)
        data_table_user_log = cursor.fetchall()
        df = pd.DataFrame(data_table_user_log,columns=['created_at','updated_at','id','user_id','action','status'])
        path_gcs = 'raw_data/user_log.csv'
        message = upload_data_to_raw_data_gcs(df ,path_gcs)
        return message
    except Exception as error :
        print ("Error while fetching data from PostgreSQL", error)
        raise error

def upload_data_to_raw_data_gcs(df ,path_gcs):
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


def main_run_get_data_from_db():
    os.path.abspath(os.getcwd())
    print(os.getcwd())
    cursor = connect_db()
    get_data_db_users(cursor)
    get_data_db_user_log(cursor)


# if __name__ == "__main__":
#     main_run_get_data_from_db()
   