from airflow import DAG

from airflow.providers.http.sensors.http import HttpSensor
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from OpenWeatherETL import weather_ETL
default_args = {
    'owner': 'Hossam',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 14),
    'email': ['mrhossamfid@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'weatherETL',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    def starter():
        airflow = """
        __  _____  _____  __  __
        /\\ \\/ / __ \\/__   \\/\\ \\/ /
        \\ \\  /\\ \\ \\/ _\\ \\  \\ \\  / 
        \\ \\_\\ \\__/\\ \\__/   \\_\\_\\ 
        \\/_/\\____/\\____/\\/_/\\_\\
        """
        print(airflow)
        print("Airflow job started working!")

    job_start = PythonOperator(
        task_id='job_start',
        python_callable=starter
    )

    # i setup pre-configured connection called 'open_weather_map_api'
    is_api_200OK = HttpSensor(
        task_id='is_api_200OK',
        http_conn_id='open_weather_map_api',
        endpoint='/data/2.5/weather?q=cairo&appid=c0810f4a8783c59ca8ccd3ce01949653'

    )

    api_key = 'c0810f4a8783c59ca8ccd3ce01949653'

    get_weather_data= PythonOperator(
        task_id='get_weather_data',
        python_callable=weather_ETL
    )


    job_start >> is_api_200OK #>> get_weather_data