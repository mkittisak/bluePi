from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.utils import timezone
from datetime import timedelta

from Ingestions.packages import get_data_from_db
from Ingestions.packages import transform_data
from Ingestions.packages import cleaned_data_to_gbq

project_id = 'de-exam-kittisak'
dwh_dataset = 'exam_kittisak'

dag = DAG('ingestion_db_to_gbq',
    default_args = {
        'onwer': 'MK',
        "email": ["kittisak.moollaong@gmail.com"],
        },
    schedule_interval='0 */1 * * *',
    start_date = timezone.datetime(2021, 2, 25),
    # end_date = timezone.datetime(2021, 2, 30),
    catchup = False)


start = DummyOperator(
    task_id = 'start',
    dag = dag,
)

get_data_db = PythonOperator(
    task_id = 'get_data_db',
    python_callable = get_data_from_db.main_run_get_data_from_db,
    dag = dag,
)

data_transform_to_cleaned_gcs = PythonOperator(
    task_id = 'data_transform_to_cleaned_gcs',
    python_callable = transform_data.main_run_transform_data_to_cleaned_gcs,
    dag = dag,
)

upload_data_to_gbq = PythonOperator(
    task_id = 'upload_data_to_gbq',
    python_callable = cleaned_data_to_gbq.main_run_data_to_gbq,
    dag = dag,
)

unique_data_users_gbq = BigQueryOperator(
    dag=dag,
    task_id='unique_data_users_gbq',
    use_legacy_sql=False,
    destination_dataset_table=f'{project_id}:{dwh_dataset}.users',
    write_disposition='WRITE_TRUNCATE',
    allow_large_results=True,
    sql='SELECT DISTINCT * FROM `de-exam-kittisak.exam_kittisak.users`')

unique_data_user_log_gbq = BigQueryOperator(
    dag=dag,
    task_id='unique_data_user_log_gbq',
    use_legacy_sql=False,
    destination_dataset_table=f'{project_id}:{dwh_dataset}.user_log',
    write_disposition='WRITE_TRUNCATE',
    allow_large_results=True,
    sql='SELECT DISTINCT * FROM `de-exam-kittisak.exam_kittisak.user_log`')


end = DummyOperator(
    task_id = 'end',
    dag = dag,
)


# Dependencies
start >> get_data_db >> data_transform_to_cleaned_gcs >> upload_data_to_gbq
upload_data_to_gbq >> [unique_data_users_gbq, unique_data_user_log_gbq] >> end