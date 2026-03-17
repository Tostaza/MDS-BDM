from datetime import datetime
import json
import kagglehub
import datasets

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.Extractors.csv import CsvExtractor  # ajusta rutas si hace falta
from src.Zona.landing_zone import landingZone
from src.Clients.seaweedfs import Seaweedfs

def task_extract_all_data(**context):
    
    
    
def task_load_data(**context):
    
    
    


def main():
    
    with DAG(
        dag_id="bdm_pipeline",
        start_date=datetime(2024, 1, 1),
        schedule_interval=None,  # o como quieras planificarlo
        catchup=False,
    ) as dag:

        extract_all_data = PythonOperator(
            task_id="extract_all_data",
            python_callable=task_extract_all_data,
            provide_context=True,
        )

        load_all_data = PythonOperator(
            task_id="load_all_data",
            python_callable=task_load_data,
            provide_context=True,
        )

        extract_all_data >> load_all_data
        
        
        

if __name__ == '__main__':
    main()