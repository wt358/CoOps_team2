from datetime import timedelta
from datetime import datetime
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

from json import loads

# define funcs

#pull raw data in the cloud and run the aug module. Then save the aug data files in the local.
def pull_mds_gan():
    now = datetime.now()
    curr_time = now.strftime("%Y-%m-%d_%H:%M:%S")

    consumer = KafkaConsumer('raw.coops2022.molding_data',
            group_id=f'airflow_{curr_time}',
            bootstrap_servers=['kafka-clust-kafka-persis-d198b-11683092-d3d89e335b84.kr.lb.naverncp.com:9094'],
            value_deserializer=lambda x: loads(x.decode('utf-8')),
            auto_offset_reset='earliest',
            consumer_timeout_ms=10000
            )
    #consumer.poll(timeout_ms=1000, max_records=2000)

    #dataframe extract
    l=[]
    
    for message in consumer:
        message = message.value
        l.append(loads(message['payload'])['fullDocument'])
    df = pd.DataFrame(l[1:])
    


    # dataframe transform
    df.drop(columns={'Barrel_Temperature_1',
            'Barrel_Temperature_2',
            'Barrel_Temperature_3',
            'Barrel_Temperature_4',
            'Barrel_Temperature_5',
            'Barrel_Temperature_6',
            'Barrel_Temperature_7',
            'Max_Injection_Speed',
            'Max_Injection_Pressure',
            'Max_Screw_RPM',
            'Max_Switch_Over_Pressure',
            'Max_Back_Pressure',
            'Clamp_open_time',
            'Mold_Temperature_1',
            'Mold_Temperature_2',
            'Mold_Temperature_3',
            'Mold_Temperature_4',
            'Mold_Temperature_5',
            'Mold_Temperature_6',
            'Mold_Temperature_7',
            'Mold_Temperature_8',
            'Mold_Temperature_9',
            'Mold_Temperature_10',
            'Mold_Temperature_11',
            'Mold_Temperature_12',
            'Hopper_Temperature',
            'Cavity',
            'NGmark',
            '_id',},inplace=True)
    print(df.head())
    print(df.shape)
    print(type(df))
    moldset_labeled_9000R=df[df.Additional_Info_1=='09520 9000R']
    moldset_labeled_9000R=moldset_labeled_9000R.reset_index(drop=True)
    print(moldset_labeled_9000R.head())
    print(len(moldset_labeled_9000R))
    
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
