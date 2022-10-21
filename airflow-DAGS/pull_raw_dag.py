from datetime import timedelta
from datetime import datetime
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models.variable import Variable
from airflow.utils.trigger_rule import TriggerRule


import influxdb_client
import csv
from pymongo import MongoClient
import pandas as pd
import os

import time

import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text

# define funcs
# 이제 여기서 15분마다 실행되게 하고, query find 할때 20분 레인지
def pull_influx():
    bucket = Variable.get("INFLUX_BUCKET")
    org = Variable.get("INFLUX_ORG")
    token = Variable.get("INFLUX_TOKEN")
    # Store the URL of your InfluxDB instance
    url= Variable.get("INFLUX_URL")


    client = influxdb_client.InfluxDBClient(
    url=url,
    token=token,
    org=org
    )

    query_api = client.query_api()

    #
    query = ' from(bucket:"cloud-bucket")\
    |> range(start: -4y)\
    |> filter(fn:(r) => r._measurement == "CLSeoGwang25HO")\
    |> filter(fn:(r) => r._field == "Weight" )'
    #|> range(start: -2mo)
    # result = query_api.query(org=org, query=query)
    # print(result)


    FILENAME = 'Weight.csv'

    influx_df =  query_api.query_data_frame(query=query)
    print(len(influx_df))
    influx_df.to_csv(os.path.join(".",FILENAME), index=False)

    reader = open(FILENAME, 'r')
    data = csv.DictReader(reader, influx_df.keys())

    mongoClient = MongoClient()
    host = Variable.get("MONGO_URL_SECRET")
    client = MongoClient(host)


    db_test = client['coops2022']
    collection_test1 = db_test['weight_data']
    try:
        result = collection_test1.insert_many(data)
    except:
        print("mongo connection failed")


def pull_mssql():
    # DB 서버 주소
    host = Variable.get("MS_HOST")
    # 데이터 베이스 이름
    database = Variable.get("MS_DATABASE")
    # 접속 유저명
    username = Variable.get("MS_USERNAME")
    # 접속 유저 패스워드
    password = Variable.get("MS_PASSWORD")
    #쿼리
    #query = text("SELECT * from shot_data WITH(NOLOCK) where TimeStamp > DATEADD(MI,-15,GETDATE())")
    query = text("SELECT * from shot_data WITH(NOLOCK) where TimeStamp < DATEADD(MI,+15,GETDATE())")
    # "SELECT * from shot_data WITH(NOLOCK) where TimeStamp > DATEADD(MONTH,-1,GETDATE())"
    #한시간 단위로 pull -> "SELECT *,DATEADD(MI,-60,GETDATE()) from shot_data WITH(NOLOCK)"
    # MSSQL 접속
    conection_url = sqlalchemy.engine.url.URL(
        drivername="mssql+pymssql",
        username=username,
        password=password,
        host=host,
        database=database,
    )
    engine = create_engine(conection_url,echo=True)

    #Session 생성
    #Session = sessionmaker(bind=engine)
    #ms_session = Session()

    FILENAME = "molding_all.csv"

    sql_result = engine.execute(query)
    sql_result_pd = pd.read_sql_query(query, engine)
    sql_result_pd.to_csv(os.path.join(".",FILENAME), index=False)

    # for i,row in enumerate(sql_result):
    #     #print(type(row)) == <class 'sqlalchemy.engine.row.LegacyRow'>
    #     for column, value in row.items():
    #         if i > 1:
    #             break
    #         print('{0}: {1}'.format(column, value))
    #         #outcsv.writerow(r)

    print("query result length: " + str(len(list(sql_result))))

    reader = open(FILENAME, 'r')
    data = csv.DictReader(reader, sql_result.keys())

    mongoClient = MongoClient()
    host = Variable.get("MONGO_URL_SECRET")
    client = MongoClient(host)


    db_test = client['coops2022']
    collection_test1 = db_test['molding_data']
    try:
        result = collection_test1.insert_many(data)
    except:
        print("mongo connection failed")

def wait_kafka():
    time.sleep(30)

def pull_transform():
    mongoClient = MongoClient()
    host = Variable.get("MONGO_URL_SECRET")
    client = MongoClient(host)


    db_test = client['coops2022']
    collection_test1 = db_test['molding_data']
    now = datetime.now()
    start = now - timedelta(days=7)
    start1 = now - timedelta(years=4)
    print(start)
    query={
            'TimeStamp':{
                '$gt':f'{start1}',
                '$lt':f'{now}'
                }
            }
    try:
        df = pd.DataFrame(list(collection_test1.find(query)))
    except Exception as e: 
        print("mongo connection failed")
     
    print(df)
    if df.empty:
        print("empty")
        return
    df.drop(columns={'_id'},inplace=True)

    df=df.drop_duplicates(subset=["idx"])
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
        },inplace=True)
    df=df[df['idx']!='idx']
    print(df.shape)
    print(df.columns)
    print(df)
    '''
    moldset_labeled_9000R=df[df.Additional_Info_1=='09520 9000R']
    print(moldset_labeled_9000R.head())
    moldset_labeled_9000R=moldset_labeled_9000R.reset_index(drop=True)
    print(moldset_labeled_9000R.head())
    print(len(moldset_labeled_9000R))
    '''
    mongoClient = MongoClient()
    host = Variable.get("MONGO_URL_SECRET")
    client = MongoClient(host)

    db_test = client['coops2022_etl']
    collection_aug=db_test['etl_data']
    data=df.to_dict('records')
    # 아래 부분은 테스트 할 때 매번 다른 oid로 데이터가 쌓이는 것을 막기 위함
    try:
        for row in data:
            uniq=row['idx']
            result = collection_aug.update_one({'idx':uniq},{"$set":row},upsert=True)
    except Exception as e: 
        print("mongo connection failed")
        print(e)
    
    print("hello")


# define DAG with 'with' phase
with DAG(
    dag_id="pull_raw_dag", # DAG의 식별자용 아이디입니다.
    description="pull raw data from local DBs", # DAG에 대해 설명합니다.
    start_date=days_ago(2), # DAG 정의 기준 2일 전부터 시작합니다.
    schedule_interval=timedelta(days=1), # 매일 00:00에 실행합니다.
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

    sleep_task = PythonOperator(
        task_id="sleep_60s",
        python_callable=wait_kafka,
        depends_on_past=True,
        owner="coops2",
        retries=0,
        retry_delay=timedelta(minutes=1),
    )

    t1 = PythonOperator(
        task_id="pull_influx",
        python_callable=pull_influx,
        depends_on_past=True,
        owner="coops2",
        retries=0,
        retry_delay=timedelta(minutes=1),
    )

    t2 = PythonOperator(
        task_id="pull_mssql",
        python_callable=pull_mssql,
        depends_on_past=True,
        owner="coops2",
        retries=0,
        retry_delay=timedelta(minutes=1),
    )
    t3 = PythonOperator(
        task_id="pull_transform",
        python_callable=pull_transform,
        depends_on_past=True,
        owner="coops2",
        retries=0,
        retry_delay=timedelta(minutes=1),
    )
   

    dummy1 = DummyOperator(task_id="path1")
    dummy2 = DummyOperator(task_id="path2",trigger_rule=TriggerRule.NONE_FAILED)
    
    # 테스크 순서를 정합니다.
    # t1 실행 후 t2를 실행합니다.
    
    dummy1 >> [t1,t2] >> dummy2

    dummy2 >> t3 >> sleep_task 
