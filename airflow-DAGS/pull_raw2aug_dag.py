from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models.variable import Variable

import csv
import pandas as pd
import os

from kafka import KafkaConsumer
from kafka import KafkaProducer


import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text

# define funcs

#pull raw data in the cloud and run the aug module. Then save the aug data files in the local.
def pull_mds_gan():
    consumer = KafkaConsumer('raw.coops2022.molding_data',
            group_id='new_group1',
            bootstrap_servers=['kafka-clust-kafka-persis-d198b-11683092-d3d89e335b84.kr.lb.naverncp.com:9094'],
            auto_offset_reset='earliest',
            consumer_timeout_ms=1000
            )
    #consumer.poll(timeout_ms=1000, max_records=2000)

    l=[]
    
    for message in consumer:
        message = message.value
        print(message)
        break
        l.append(message)

    df = pd.DataFrame(l)
    print(df.head())
    print("hello")


#provide the aug data that saved in the local to the aug topic in the kafka cluster
def provide_aug_data():
    print()


# define DAG with 'with' phase
with DAG(
    dag_id="pull_raw2aug_dag", # DAG의 식별자용 아이디입니다.
    description="pull raw data in the cloud to aug module", # DAG에 대해 설명합니다.
    start_date=days_ago(2), # DAG 정의 기준 2일 전부터 시작합니다.
    schedule_interval=timedelta(days=7), # 매주 00:00에 실행합니다.
    tags=["my_dags"],
    ) as dag:
# define the tasks

#t = BashOperator(
#    task_id="print_hello",
#    bash_command="echo Hello",
#    owner="", # 이 작업의 오너입니다. 보통 작업을 담당하는 사람 이름을 넣습니다.
#    retries=3, # 이 테스크가 실패한 경우, 3번 재시도 합니다.
#    retry_delay=timedelta(minutes=5), # 재시도하는 시간 간격은 5분입니다.
#)


    t1 = PythonOperator(
        task_id="pull_mds_gan",
        python_callable=pull_mds_gan,
        depends_on_past=True,
        owner="coops2",
        retries=3,
        retry_delay=timedelta(minutes=1),
    )

    t2 = PythonOperator(
        task_id="provide_aug_data",
        python_callable=provide_aug_data,
        depends_on_past=True,
        owner="coops2",
        retries=3,
        retry_delay=timedelta(minutes=1),
    )
    # 테스크 순서를 정합니다.
    # t1 실행 후 t2를 실행합니다.
    t1 >> t2
