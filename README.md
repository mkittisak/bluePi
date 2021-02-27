# bluePi
## Create dags for run schedule every 1 hour 
### bluePi/dags/dags_pipeline_db_to_gbq.py 

สิ่งที่ประกอบในdagsจะมีpackages function ต่างๆไว้สำหรับ ETL และจะต่อ Dependencies กันตามtask

## Installation

Use the package in [airflow]

```airflow
import DAG
import DummyOperator
import BashOperator
import PythonOperator
import BigQueryOperator
import timezone
```

## Usage

```airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.utils import timezone
```

