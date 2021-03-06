# bluePi
## Create dags for run schedule every 1 hour 
### bluePi/dags/dags_pipeline_db_to_gbq.py 

สิ่งที่ประกอบในdagsจะมีpackages function ต่างๆไว้สำหรับ ETL และจะต่อ Dependencies กันตามtask

## Installation
### Install requirements.txt
A requirements file to install from. This option can replace the packages input or editable input, but either this, the packages option, or the editable option is required.

The input value should be a valid requirements file to install from. It corresponds to the --requirement argument for pip. Any valid value for that argument will be valid for this option.

No special formatting is done with this input, so it won't be treated special. The value recieved from this input will be passed directly to pip.

```python
pip3 install -r requirements.txt
```

## Installation

Use the package in dags[airflow]

```python
import DAG
import DummyOperator
import BashOperator
import PythonOperator
import BigQueryOperator
import timezone
```

## Usage

```python
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.utils import timezone
```

## Installation

Use the package in [pip](https://pip.pypa.io/en/stable/) for run get_data_from_db.py

```python
import psycopg2 
import pandas as pd
import csv
import os
from configparser import ConfigParser
import configparser
from google.cloud import storage
from google.cloud.storage import blob
```

## Installation

Use the package in [pip](https://pip.pypa.io/en/stable/) for run transform_data.py

```python
import pandas as pd
from io import StringIO
import csv
from google.cloud import storage
from google.cloud.storage import blob
```

## Installation

Use the package in [pip](https://pip.pypa.io/en/stable/) for run cleaned_data_to_gbq.py

```python
from pandas.io import gbq
from io import StringIO
import pandas as pd
from google.cloud import storage
from google.cloud.storage import blob
```

## Installation

Use the package in [pip](https://pip.pypa.io/en/stable/) for run export_data_to_dataproc_staging.py

```python
import pandas as pd
import os
from io import StringIO
from google.cloud import storage
from google.cloud.storage import blob
from google.cloud import bigquery
```