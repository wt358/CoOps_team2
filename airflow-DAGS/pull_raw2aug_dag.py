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
def customize(dataframe,mds_matrix):
    answer=pd.DataFrame(np.zeros(len(mds_matrix)))
    answer.rename(columns = {0:'Class'},inplace=True)## 일단 전부 양품으로 간주하느 데이터프레임 생성
    for i in range(1,10):
        for j in range(1,10):
            dbscan=DBSCAN(eps = i,min_samples=j)
            clusters_mds = pd.DataFrame(dbscan.fit_predict(mds_matrix))
            clusters_mds.rename(columns = {0:'Class'},inplace=True)
            if clusters_mds.value_counts().count()==2:
                if len(clusters_mds.loc[clusters_mds['Class'] == -1]) >  len(answer.loc[answer['Class'] == -1]): 
                    answer=clusters_mds
    dataframe.reset_index(inplace=True,drop=True)          
    result = pd.DataFrame(pd.concat([dataframe,answer], axis = 1))       
    return result

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
    print(df.columns)
    '''
    moldset_labeled_9000R=df[df.Additional_Info_1=='09520 9000R']
    print(moldset_labeled_9000R.head())
    moldset_labeled_9000R=moldset_labeled_9000R.reset_index(drop=True)
    print(moldset_labeled_9000R.head())
    print(len(moldset_labeled_9000R))
    '''
    #IQR
    print(df)
    section=pd.to_numeric(df,errors = 'coerce')
    section=section.reset_index(drop=True)
    print(df)
    for i in range(5,17):
        level_1q = section.iloc[:,i].quantile(0.025)
        level_3q = section.iloc[:,i].quantile(0.975)
        IQR = level_3q - level_1q
        rev_range = 1.5 # 제거 범위 조절 변수
        section = section[(section.iloc[:,i] <= level_3q + (rev_range * IQR)) & (section.iloc[:,i] >= level_1q - (rev_range * IQR))] ## sectiond에 저장된 데이터 프레임의 이상치 제거 작업
    print(section)
    last_idx = 0
    curr_idx = 0

    # 자른 데이터프레임을 저장할 리스트
    pds = []

    for idx in range(section.index.start + 1, section.index.stop):
        # print(idx)
        # print(moldset_labeled_9000R.loc[idx,'TimeStamp'])
        time_to_compare1 = datetime.strptime(section.loc[idx,'TimeStamp'], "%Y-%m-%d %H:%M:%S")
        time_to_compare2 = datetime.strptime(section.loc[idx-1,'TimeStamp'], "%Y-%m-%d %H:%M:%S")
        time_diff = time_to_compare1 - time_to_compare2

        # 분 단위로 비교
        if time_diff.seconds / 60 > 15:
            curr_idx = idx -1
            pds.append(section.truncate(before=last_idx, after=curr_idx,axis=0))
            last_idx = idx

    else:
        pds.append(section.truncate(before=last_idx, after=section.index.stop - 1,axis=0))




    list1=[]## 불량여부가 라벨링된 구간별 데이터 프레임을 저장할 리스트

    for i in range(len(pds)):
        start_time = time.time()
        dataframe=pds[i].iloc[:,6:] ##i번째 구간을 불러와서 저장
        print('%d 번째' %i)
        if len(pds[i])>=30:
            std = StandardScaler().fit_transform(dataframe) ## 정규화 진행
            end_time = time.time()
            print('    if std 코드 실행 시간: %20ds' % (end_time - start_time))
            mds_results = MDS(n_components=2).fit_transform(std) ## mds차원축소결과 저장(시간이 좀 많이 소요됨)
            end_time = time.time()
            print('    if mds 코드 실행 시간: %20ds' % (end_time - start_time))
            mds_results=pd.DataFrame(mds_results) ##dataframe 형태로 저장 
            end_time = time.time()
            print('    if df 코드 실행 시간: %20ds' % (end_time - start_time))
            list1.append(customize(dataframe,mds_results))## 구간별 라벨링 데이터 프레임을 리스트에 저장
            end_time = time.time()
            print('    if 코드 실행 시간: %20ds' % (end_time - start_time))
        else :
            answer=pd.DataFrame(np.zeros(len(pds[i]))) 
            answer.rename(columns = {0:'Class'},inplace=True) 
            dataframe.reset_index(inplace=True,drop=True)     
            result = pd.DataFrame(pd.concat([dataframe,answer], axis = 1))
            list1.append(result)
            end_time = time.time()
            print('    else 코드 실행 시간: %20ds' % (end_time - start_time))
    df_all=pd.concat(list1, ignore_index=True)



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
