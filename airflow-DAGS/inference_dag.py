from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models.variable import Variable


import influxdb_client
import csv
from pymongo import MongoClient
import pandas as pd
import os

import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text

# define funcs
def model_inference():
    # pull the ETL data
    mongoClient = MongoClient()
    host = Variable.get("MONGO_URL_SECRET")
    client = MongoClient(host)
    
    db_test = client['coops2022_etl']
    collection_aug=db_test['etl_data']
    
    try:
        df = pd.DataFrame(list(collection_aug.find()))
        
    except:
        print("mongo connection failed")
        return False
    df.drop(columns={'_id',
        },inplace=True)

    print(df)




# define DAG with 'with' phase
with DAG(
    dag_id="inference_dag", # DAG의 식별자용 아이디입니다.
    description="Model deploy and inference", # DAG에 대해 설명합니다.
    start_date=days_ago(2), # DAG 정의 기준 2일 전부터 시작합니다.
    schedule_interval=timedelta(minutes=15), # 매일 00:00에 실행합니다.
    tags=["inference"],
    ) as dag:

    t1 = PythonOperator(
        task_id="model_inference",
        python_callable=model_inference,
        depends_on_past=True,
        owner="coops2",
        retries=0,
        retry_delay=timedelta(minutes=1),
    )
    '''
    t2 = PythonOperator(
        task_id="pull_mssql",
        python_callable=pull_mssql,
        depends_on_past=True,
        owner="coops2",
        retries=3,
        retry_delay=timedelta(minutes=1),
    )
    '''
    # 테스크 순서를 정합니다.
    # t1 실행 후 t2를 실행합니다.
     t1
