# Airflow Orchestration done from Docker
# Importing relevant libraries
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.http.sensors.http import HttpSensor
import json
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
import pandas as pd


# Data extracted from extract_currency_exchange_data task
data = []

# function to extract data from json
def transform_data(task_instance):
    currency = task_instance.xcom_pull(task_ids = 'extract_currency_exchange_data')# pull data from the previous task
    timestamp = currency['timestamp']
    timestamp = int(datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%SZ').timestamp())# Convert timestamp string to Unix timestamp (integer)
    currency_from = currency['from']
    for item in currency['to']:
        USD_to_currency_rate = item['mid']
        currency_to_USD_rate = item['inverse']
        currency_to = item['quotecurrency']
        transformed_data = {
            'timestamp': timestamp,
            'currency_from': currency_from,
            'USD_to_currency_rate': USD_to_currency_rate,
            'currency_to_USD_rate': currency_to_USD_rate,
            'currency_to': currency_to
        }
        data.append(transformed_data) # Appending data into the data
    
    # transforming data into a Pandas dataframe 
    df = pd.DataFrame(data)
    
    # naming the file output
    now = datetime.now()
    dt_string = now.strftime('%d%m%Y%H%M%S')
    dt_string = 'current_exchange_rate_' + dt_string
    df.to_csv(f'{dt_string}.csv', index = False, header=False)

# Define the default_args 
default_args = {
	'owner' : 'tomiwa',
	'depends_on_past' : False,
	'start_date' : datetime(2023, 2, 8),
	'email' : ['tomiwa.aladebo@yahoo.com'],
	'email_on_failure' : False,
	'email_on_retry' : False,
	'retries' : 2,
	'retry_delay' : timedelta(minutes=2)
	}
# Defining DAGS
with DAG('Currency_exchange',
         default_args=default_args,
         schedule_interval='0 1,23 * * *',  # Scheduled 1:00 AM and 11:00 PM daily
         catchup=False) as dag:
    # Dummy pipeline to start the pipeline
    Start_pipeline = DummyOperator(
        task_id='Start_pipeline'
    )
    # Pipeline to check the connection
    is_currency_exchange_api_Connecting = HttpSensor(
        task_id='is_currency_exchange_api_Connecting',
        http_conn_id='currency_data',
        endpoint='/v1/convert_from.json/?from=USD&to=NGN,GHS,KES,UGX,MAD,XOF,EGP&amount=1&inverse=True'
    )
    # Pipeline to extract data
    extract_currency_exchange_data = SimpleHttpOperator(
        task_id='extract_currency_exchange_data',
        http_conn_id='currency_data',
        endpoint='/v1/convert_from.json/?from=USD&to=NGN,GHS,KES,UGX,MAD,XOF,EGP&amount=1&inverse=True',
        method='GET',
        response_filter=lambda r: json.loads(r.text),
        log_response=True
    )
    # Pipeline to transform data into pandas dataframe
    transform_currency_exchange_data = PythonOperator(
        task_id='transform_currency_exchange_data',
        python_callable=transform_data,  # python callable is to call a function (pulls data from the previous task)
        provide_context=True
    )


# Sequence of task
Start_pipeline >> is_currency_exchange_api_Connecting >> extract_currency_exchange_data >> transform_currency_exchange_data