from datetime import timedelta
from datetime import datetime
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models.variable import Variable
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn.decomposition import PCA
from sklearn.cluster import DBSCAN
from sklearn.manifold import MDS
from sklearn.metrics import precision_score, recall_score, f1_score,accuracy_score, classification_report, plot_confusion_matrix, confusion_matrix

from sklearn.model_selection import KFold, GridSearchCV
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import IsolationForest
from sklearn.svm import OneClassSVM 
from sklearn.metrics import confusion_matrix, roc_curve, roc_auc_score

from IPython.display import Image
import matplotlib.pyplot as plt
import seaborn as sns


from bson import ObjectId

import gridfs
import io

from gridfs import GridFS



from imblearn.over_sampling import SMOTE
from imblearn.pipeline import Pipeline

from tensorflow.keras.layers import Input, Dense, Reshape, Flatten, Dropout, multiply, Concatenate, Dropout, LSTM, TimeDistributed, RepeatVector
from tensorflow.keras.layers import BatchNormalization, Activation, Embedding, ZeroPadding2D, LeakyReLU
from tensorflow.keras.models import Sequential, Model
from tensorflow.keras.optimizers import Adam, RMSprop
from tensorflow.keras.initializers import RandomNormal
import tensorflow.keras.backend as K
from tensorflow.keras import regularizers
from tensorflow.python.client import device_lib
from sklearn.utils import shuffle
import tensorflow as tf

import joblib


tf.compat.v1.logging.set_verbosity(tf.compat.v1.logging.ERROR)

import influxdb_client
import csv
import pandas as pd
import os
import time
import numpy as np
from collections import Counter
from kafka import KafkaConsumer
from kafka import KafkaProducer
from pymongo import MongoClient

import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text

from json import loads
import random as rn

np.random.seed(34)

# manual parameters
RANDOM_SEED = 42
TRAINING_SAMPLE = 50000
VALIDATE_SIZE = 0.2

# setting random seeds for libraries to ensure reproducibility
np.random.seed(RANDOM_SEED)
rn.seed(RANDOM_SEED)
tf.random.set_seed(RANDOM_SEED)

rn.seed(10)

tf.random.set_seed(10)
class ModelSingleton(type):
   """
   Metaclass that creates a Singleton base type when called.
   """
   _mongo_id = {}
   def __call__(cls, *args, **kwargs):
       mongo_id = kwargs['mongo_id']
       if mongo_id not in cls._mongo_id:
           print('Adding model into ModelSingleton')
           cls._mongo_id[mongo_id] = super(ModelSingleton, cls).__call__(*args, **kwargs)
       return cls._mongo_id[mongo_id]

class LoadModel(metaclass=ModelSingleton):
   def __init__(self, *args, **kwargs):
       print(kwargs)
       self.mongo_id = kwargs['mongo_id']
       self.clf = self.load_model()
   def load_model(self):
       print('loading model')

       mongoClient = MongoClient()
       host = Variable.get("MONGO_URL_SECRET")
       client = MongoClient(host)

       db_model = client['coops2022_model']
       fs = gridfs.GridFS(db_model)
       print(self.mongo_id)
       f = fs.find({"_id": ObjectId(self.mongo_id)}).next()
       print(f)

       with open(f'{f.model_name}.joblib', 'wb') as outfile:
           outfile.write(f.read())
       return joblib.load(f'{f.model_name}.joblib')

# define funcs
def model_inference():
    #data consumer
    now = datetime.now()
    curr_time = now.strftime("%Y-%m-%d_%H:%M:%S")

    consumer = KafkaConsumer('test.coops2022_etl.etl_data',
            group_id=f'Inference_model_{curr_time}',
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
    df = pd.DataFrame(l)
    print(df)
    if df.empty:
        print("empty queue")
        return
    # dataframe transform
    df=df[df['idx']!='idx']
    print(df.shape)
    print(df.columns)
    print(df)

    df.drop(columns={'_id',
        },inplace=True)
    
    print(df)

    
    labled = pd.DataFrame(df, columns = ['Filling_Time','Plasticizing_Time','Cycle_Time','Cushion_Position'])


    labled.columns = map(str.lower,labled.columns)

    print(labled.head())

    X_test = labled.sample(frac=1)
    test = X_test 
    X_test=X_test.values
    print(f"""Shape of the datasets:
        Testing  (rows, cols) = {X_test.shape}""")
    
    
    # transforming data from the time domain to the frequency domain using fast Fourier transform
    test_fft = np.fft.fft(X_test)
    
    scaler = MinMaxScaler()
    X_test = scaler.fit_transform(X_test)# 나중에 scaler도 pull raw 2 aug에서 모델을 저장해서 놓고 여기서는 그 모델을 불러와서 transform(X_test)만 해야함.
    scaler_filename = "scaler_data"
    joblib.dump(scaler, scaler_filename)

    X_test = X_test.reshape(X_test.shape[0], 1, X_test.shape[1])
    print("Test data shape:", X_test.shape)
    #model load    
    mongoClient = MongoClient()
    host = Variable.get("MONGO_URL_SECRET")
    client = MongoClient(host)
    
    db_model = client['coops2022_model']
    fs = gridfs.GridFS(db_model)
    collection_model=db_model['mongo_LSTM_autoencoder']
    
    model_name = 'LSTM_autoencoder'
    model_fpath = f'{model_name}.joblib'
    result = collection_model.find({"model_name": model_name}).sort('uploadDate', -1)
    
    print(result)
    if len(list(result.clone()))==0:
        print("empty")
        return 1
    else:
        print("not empty")
        model = LoadModel(mongo_id=file_id).clf

    joblib.dump(model, model_fpath)
    
    model.compile(optimizer='adam', loss='mae')
    
    # 이상값은 -1으로 나타낸다.
    print(model.summary())


    X_pred = model.predict(X_test)
    X_pred = X_pred.reshape(X_pred.shape[0], X_pred.shape[2])
    X_pred = pd.DataFrame(X_pred, columns=test.columns)
    X_pred.index = test.index
    
    scored = pd.DataFrame(index=test.index)
    Xtest = X_test.reshape(X_test.shape[0], X_test.shape[2])
    scored['Loss_mae'] = np.mean(np.abs(X_pred-Xtest), axis = 1)
    scored['Threshold'] = 0.1
    scored['Anomaly'] = scored['Loss_mae'] > scored['Threshold']
    scored['label'] = labled['label']
    print(scored.head())

    y_test = scored['Anomaly']
    print(y_test.unique())

    print(scored[scored['Anomaly']==True].label.count())
    print(scored.label.unique())

    outliers = scored['label']
    outliers = outliers.fillna(0)
    print(outliers.unique())

    outliers = outliers.to_numpy()
    y_test = y_test.to_numpy()
    print(y_test)
    cm = confusion_matrix(y_test, outliers)
    (tn, fp, fn, tp) = cm.flatten()


    print(f"""{cm}
    % of transactions labeled as fraud that were correct (precision): {tp}/({fp}+{tp}) = {tp/(fp+tp):.2%}
    % of fraudulent transactions were caught succesfully (recall):    {tp}/({fn}+{tp}) = {tp/(fn+tp):.2%}
    % of g-mean value : root of (specificity)*(recall) = ({tn}/({fp}+{tn})*{tp}/({fn}+{tp})) = {(tn/(fp+tn)*tp/(fn+tp))**0.5 :.2%}""")

    print(roc_auc_score(outliers, y_test))


    print("hello inference")




# define DAG with 'with' phase
with DAG(
    dag_id="inference_dag", # DAG의 식별자용 아이디입니다.
    description="Model deploy and inference", # DAG에 대해 설명합니다.
    start_date=days_ago(2), # DAG 정의 기준 2일 전부터 시작합니다.
    schedule_interval=timedelta(days=1), # 매일 00:00에 실행합니다.
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
    dummy1 = DummyOperator(task_id="path1")
    # 테스크 순서를 정합니다.
    # t1 실행 후 t2를 실행합니다.
    dummy1 >> t1
