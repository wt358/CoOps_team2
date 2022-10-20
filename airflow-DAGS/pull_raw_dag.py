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
    |> range(start: -15m)\
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
    query = text("SELECT * from shot_data WITH(NOLOCK) where TimeStamp > DATEADD(MI,-15,GETDATE())")
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
    time.sleep(60)

def pull_transform():
    mongoClient = MongoClient()
    host = Variable.get("MONGO_URL_SECRET")
    client = MongoClient(host)


    db_test = client['coops2022']
    collection_test1 = db_test['molding_data']
    try:
        df = pd.DataFrame(list(collection_test1.find()))
    except:
        print("mongo connection failed")
     
    print(df) 


# define DAG with 'with' phase
with DAG(
    dag_id="pull_raw_dag", # DAG의 식별자용 아이디입니다.
    description="pull raw data from local DBs", # DAG에 대해 설명합니다.
    start_date=days_ago(2), # DAG 정의 기준 2일 전부터 시작합니다.
    schedule_interval=timedelta(minutes=15), # 매일 00:00에 실행합니다.
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
    
    
    # 테스크 순서를 정합니다.
    # t1 실행 후 t2를 실행합니다.
    
    [t2,t1] >> t3 >> sleep_task
