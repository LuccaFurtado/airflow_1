from airflow import DAG
from airflow.operators.python import PythonOperator
import airflow
from bs4 import BeautifulSoup as bs
from utils import save_csv, concat_dataframes,transform_dataframe



args = {
    'owner': 'airflow',
    'start_date' : airflow.utils.dates.days_ago(1),
}

dag = DAG(
    dag_id='colect_and_store_coins_data',
    default_args=args,
    schedule_interval= '*/15 * * * *',
)

csv_scracpping = PythonOperator(
    task_id='save_csv_file',
    python_callable=save_csv,
    dag=dag
)

csv_increment = PythonOperator(
    task_id='concat_csv',
    depends_on_past=False,
    python_callable=concat_dataframes,
    retries=3,
    dag=dag
)

csv_transform = PythonOperator(
    task_id='transform_csv',
    depends_on_past=False,
    python_callable=transform_dataframe,
    dag=dag
)


csv_scracpping >> csv_increment >> csv_transform