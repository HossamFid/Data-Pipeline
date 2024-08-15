from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator 

#from airflow.providers.smtp.operators.smtp import SmtpEmailOperator

from datetime import datetime, timedelta
from OpenWeatherETL import weather_ETL
import cities



default_args = {
    'owner': 'Hossam',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 14),
    'email': ['samxfid@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'email_on_success': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'weatherETL',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:



    def starter():
        apache_airflow_job = """
        AAAAA   PPPPP    AAAAA    CCCCC   H    H  EEEEE           AAAAA  III  RRRRR   FFFFF  L       OOOOO  W     W
        A     A  P    P  A     A  C        H    H  E               A     A  I   R    R  F      L      O     O  W     W
        AAAAAAA  PPPPP   AAAAAAA  C        HHHHHH  EEEEE           AAAAAAA  I   RRRRR   FFFFF  L      O     O  W  W  W
        A     A  P       A     A  C        H    H  E               A     A  I   R    R  F      L      O     O   W W W
        A     A  P       A     A   CCCCC   H    H  EEEEE           A     A  III  R    R  F      LLLLL   OOOOO     W W
        """

        hossam_fid_signature = """
        Written by Hossam Fid
        """

        print(apache_airflow_job)
        print(hossam_fid_signature)
        print("Apache Airflow job started working! .........")


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

    # read_cities = BashOperator(
    #     task_id = 'read_cities',
    #     bash_command='cat cities.txt'

    # )


# ... other code in your DAG ...

    # send_email_task = SmtpEmailOperator(
    # task_id='send_email',
    # to='samxfid@gmail.com',  # Replace with your recipient email address
    # # cc=[],  # Optional: List of CC email addresses
    # # bcc=[],  # Optional: List of BCC email addresses
    # subject='Airflow Notification',
    # html_content='This is an email notification from Airflow.',
    # mime_type='text/html',  # Set MIME type (text/plain for plain text)
    # from_email='penfinaltest@gmail.com',
    # smtp_conn_id='smtp_default'  # Set the SMTP connection ID (explained later)
    # )

    api_key = 'c0810f4a8783c59ca8ccd3ce01949653'

    get_weather_data= PythonOperator(
        task_id='get_weather_data',
        python_callable=weather_ETL
    )


    job_start >> is_api_200OK >>get_weather_data #>> send_email_task # 