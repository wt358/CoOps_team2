from datetime import timedelta
from datetime import datetime

from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn.decomposition import PCA
from sklearn.metrics import precision_score, recall_score, f1_score,accuracy_score, classification_report,  confusion_matrix

from sklearn.model_selection import KFold, GridSearchCV
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import IsolationForest
from sklearn.svm import OneClassSVM 
from sklearn.metrics import confusion_matrix, roc_curve, roc_auc_score
from gan import buildGAN
from preprocess import customize, IQR, MDS_molding
from loadmodel import *
# from IPython.display import Image
# import matplotlib.pyplot as plt
# import seaborn as sns


from bson import ObjectId

import gridfs
import io

from gridfs import GridFS



from imblearn.over_sampling import SMOTE
from imblearn.pipeline import Pipeline

from tensorflow.python.client import device_lib
from sklearn.utils import shuffle
import tensorflow as tf
from tensorflow.keras import regularizers
from tensorflow.keras.layers import Input, Dense, LSTM, TimeDistributed, RepeatVector
from tensorflow.keras.layers import BatchNormalization, Activation, Embedding, ZeroPadding2D, LeakyReLU
from tensorflow.keras.initializers import RandomNormal
from tensorflow.keras.models import  Model,Sequential

import joblib

tf.compat.v1.logging.set_verbosity(tf.compat.v1.logging.ERROR)

import csv
import pandas as pd
import os
import sys

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
RANDOM_SEED = int(os.environ['RANDOM_SEED'])
TRAINING_SAMPLE = int(os.environ['TRAINING_SAMPLE'])
VALIDATE_SIZE = float(os.environ['VALIDATE_SIZE'])

# setting random seeds for libraries to ensure reproducibility
np.random.seed(RANDOM_SEED)
rn.seed(RANDOM_SEED)
tf.random.set_seed(RANDOM_SEED)

rn.seed(10)

tf.random.set_seed(10)

# define funcs
def autoencoder_model(X):
    inputs = Input(shape=(X.shape[1], X.shape[2]))
    L1 = LSTM(16, activation='relu', return_sequences=True, kernel_regularizer=regularizers.l2(0.00))(inputs)
    L2 = LSTM(4, activation='relu', return_sequences=False)(L1)
    L3 = RepeatVector(X.shape[1])(L2)
    L4 = LSTM(4, activation='relu', return_sequences=True)(L3)
    L5 = LSTM(16, activation='relu', return_sequences=True)(L4)
    output = TimeDistributed(Dense(X.shape[2]))(L5)    
    model = Model(inputs=inputs, outputs=output)
    return model


#pull raw data in the cloud and run the aug module. Then save the aug data files in the local.

def iqr_mds_gan():
    now = datetime.now()
    curr_time = now.strftime("%Y-%m-%d_%H:%M:%S")

    consumer = KafkaConsumer('test.coops2022_etl.etl_data',
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
    df = pd.DataFrame(l)
    print(df)
    # dataframe transform
    df=df[df['idx']!='idx']
    print(df.shape)
    print(df.columns)
    print(df)

    df=df[df['Additional_Info_1'].str.contains("9000a")]
    df.drop(columns={'_id',
        },inplace=True)

    df=df[['idx', 'Machine_Name','Additional_Info_1', 'Additional_Info_2','Shot_Number','TimeStamp',
            'Average_Back_Pressure', 'Average_Screw_RPM', 'Clamp_Close_Time',
            'Clamp_Open_Position', 'Cushion_Position', 'Cycle_Time', 'Filling_Time',
            'Injection_Time', 'Plasticizing_Position',
            'Plasticizing_Time', 'Switch_Over_Position',
            ]]
    #IQR
    print(df)
    print(df.dtypes)
    
    df=df.reset_index(drop=True)
    section=df
    section=IQR(section)
    print(section)

    # data frame 자르기
    last_idx = 0
    curr_idx = 0

    # 자른 데이터프레임을 저장할 리스트
    pds = []
    section=section.reset_index(drop=True)
    print(section.index.tolist())
    for idx in range(1,len(section.index.tolist())):
        # print(moldset_labeled_9000R.loc[idx,'TimeStamp'])
        time_to_compare1 = datetime.strptime(section.loc[idx,'TimeStamp'], "%Y-%m-%d %H:%M:%S")
        time_to_compare2 = datetime.strptime(section.loc[idx-1,'TimeStamp'], "%Y-%m-%d %H:%M:%S")
        time_diff = time_to_compare1 - time_to_compare2

        # 분 단위로 비교
        if time_diff.seconds / 60 > 15:
            curr_idx = idx-1
            pds.append(section.truncate(before=last_idx, after=curr_idx,axis=0))
            last_idx = idx

    else:
        pds.append(section.truncate(before=last_idx, after=len(section.index.tolist())-1,axis=0))

    for i in range(len(pds)):
        print(i, pds[i].count().max())

    print(pds[0])
    df_all=MDS_molding(pds)

    print(df_all)
    print(df_all.columns)

    #GAN

    df=df_all
    df['Class'] = df_all['Class'].map(lambda x: 1 if x == -1 else 0)

    print(df)
    print(df['Class'].value_counts(normalize=True)*100)

    print(f"Number of Null values: {df.isnull().any().sum()}")

    print(f"Dataset has {df.duplicated().sum()} duplicate rows")

    df=df.dropna()
    df.drop_duplicates(inplace=True)
    try:
        df.drop(columns={'Labeling'}
                ,inplace=True)
    except:
        print("passed")
    

    print(df)

    # checking skewness of other columns

    print(df.drop('Class',1).skew())
    
    # skew_cols = df.iloc[:,5:].drop('Class',1).skew().loc[lambda x: x>2].index
    # print(skew_cols)

    # print(device_lib.list_local_devices())
    # print(tf.config.list_physical_devices())
    
    with tf.device("/gpu:0"):
    #     for col in skew_cols:
    #         lower_lim = abs(df[col].min())
    #         normal_col = df[col].apply(lambda x: np.log10(x+lower_lim+1))
    #         print(f"Skew value of {col} after log transform: {normal_col.skew()}")
    
    #     scaler = StandardScaler()
    #     #scaler = MinMaxScaler()
    #     X = scaler.fit_transform(df.iloc[:,5:].drop('Class', 1))
    #     y = df['Class'].values
    #     print(X.shape, y.shape)

    #     X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, stratify=y)


    #     gan = buildGAN(out_shape=X_train.shape[1], num_classes=2)
    #     # cgan.out_shape=X_train.shape[1]

    #     y_train = y_train.reshape(-1,1)
    #     pos_index = np.where(y_train==1)[0]
    #     neg_index = np.where(y_train==0)[0]
    #     gan.train(X_train, y_train, pos_index, neg_index, epochs=50)#원래 epochs= 5000

    #     print(df.shape)
    #     print(X_train.shape)
    #     gan_num=df.shape[0]
    #     noise = np.random.normal(0, 1, (gan_num, 32))
    #     sampled_labels = np.ones(gan_num).reshape(-1, 1)

    #     gen_samples = gan.generator.predict([noise, sampled_labels])
    #     gen_samples = scaler.inverse_transform(gen_samples)
    #     print(gen_samples.shape)

    #     gen_df = pd.DataFrame(data = gen_samples,
    #             columns = df.iloc[:,5:].drop('Class',1).columns)
    #     gen_df['Class'] = 1
    #     print(gen_df)

    #     Class0 = df[df['Class'] == 0 ]
    #     print(Class0)

    #     pca = PCA(n_components = 2)
    #     PC = pca.fit_transform(gen_df)
    #     PCdf = pca.fit_transform(Class0.iloc[:,5:])

    #     VarRatio = pca.explained_variance_ratio_
    #     VarRatio = pd.DataFrame(np.round_(VarRatio,3))

    #     CumVarRatio    = np.cumsum(pca.explained_variance_ratio_)
    #     CumVarRatio_df = pd.DataFrame(np.round_(CumVarRatio,3))

    #     Result = pd.concat([VarRatio , CumVarRatio_df], axis=1)
    #     print(pd.DataFrame(Result))
    #     print(pd.DataFrame(PC))

    #     pca3 = PCA(n_components = 3)
    #     PC3 = pca3.fit_transform(gen_df)
    #     PC_df = pca3.fit_transform(Class0.iloc[:,5:])

    #     VarRatio3 = pca3.explained_variance_ratio_
    #     VarRatio3 = pd.DataFrame(np.round_(VarRatio3,3))

    #     CumVarRatio3    = np.cumsum(pca3.explained_variance_ratio_)
    #     CumVarRatio_df3 = pd.DataFrame(np.round_(CumVarRatio3,3))

    #     Result3 = pd.concat([VarRatio3 , CumVarRatio_df3], axis=1)
    #     print(pd.DataFrame(Result3))
    #     print(pd.DataFrame(PC3))

    #     augdata = pd.concat([pd.DataFrame(Class0), gen_df])
    #     Augdata = augdata.reset_index(drop=True)
    #     print(Augdata)
    #     print(Augdata['Class'].value_counts(normalize=True)*100)
    #     Augdata['TimeStamp']=pd.to_datetime(Augdata['TimeStamp'],unit='s')
        Augdata=df
        mongoClient = MongoClient()
        #host = Variable.get("MONGO_URL_SECRET")
        host = os.environ['MONGO_URL_SECRET'] 
        client = MongoClient(host)

        db_test = client['coops2022_aug']
        collection_aug=db_test['mongo_aug1']
        data=Augdata.to_dict('records')
    # 아래 부분은 테스트 할 때 매번 다른 oid로 데이터가 쌓이는 것을 막기 위함
        try:
            isData = collection_aug.find_one()
            if len(isData) !=0:
                print("collection is not empty")
                collection_aug.delete_many({})
            try:
                result = collection_aug.insert_many(data,ordered=False)
            except Exception as e:
                print("mongo connection failed", e)
        except:
            print("there is no collection")
            try:
                result = collection_aug.insert_many(data,ordered=False)
            except Exception as e:
                print("mongo connection failed", e)
        client.close()
    print("hello")


#provide the aug data that saved in the local to the aug topic in the kafka cluster
def oc_svm():
    
    mongoClient = MongoClient()
    #host = Variable.get("MONGO_URL_SECRET")
    host = os.environ['MONGO_URL_SECRET'] 
    client = MongoClient(host)
    
    db_test = client['coops2022_aug']
    collection_aug=db_test['mongo_aug1']
    
    try:
        moldset_df = pd.DataFrame(list(collection_aug.find()))
        
    except:
        print("mongo connection failed")
        return False
    
    print(moldset_df)

    moldset_9000R=moldset_df
    

    labled = pd.DataFrame(moldset_9000R, columns = ['Filling_Time','Plasticizing_Time','Cycle_Time','Cushion_Position','Class'])
    labled.columns = map(str.lower,labled.columns)
    labled.rename(columns={'class':'label'},inplace=True)
    print(labled.head())

    target_columns = pd.DataFrame(labled, columns = ['cycle_time', 'cushion_position'])
    target_columns.astype('float')
     
    db_model = client['coops2022_model']
    fs = gridfs.GridFS(db_model)
    collection_model=db_model['mongo_OCSVM']
    
    model_name = 'OC_SVM'
    model_fpath = f'{model_name}.joblib'
    result = collection_model.find({"model_name": model_name}).sort([("uploadDate", -1)])
    print(result)
    cnt=result.count()
    print(result[0])
    print(result[cnt])
    if len(list(result.clone()))==0:
        print("empty")
        model = OneClassSVM(kernel = 'rbf', gamma = 0.001, nu = 0.04).fit(target_columns)
    else:
        print("not empty")
        file_id = str(result[cnt]['file_id'])

        model = LoadModel(mongo_id=file_id).clf
    joblib.dump(model, model_fpath)
    
    print(model.get_params())
    
    y_pred = model.predict(target_columns)
    print(y_pred)



    # filter outlier index
    outlier_index = np.where(y_pred == -1)

    #filter outlier values
    outlier_values = target_columns.iloc[outlier_index]
    print(outlier_values)
    
    # 이상값은 -1으로 나타낸다.
    score = model.fit(target_columns)
    anomaly = model.predict(target_columns)
    target_columns['anomaly']= anomaly
    anomaly_data = target_columns.loc[target_columns['anomaly']==-1] 
    print(target_columns['anomaly'].value_counts())

    target_columns[target_columns['anomaly']==1] = 0
    target_columns[target_columns['anomaly']==-1] = 1
    target_columns['Anomaly'] = target_columns['anomaly'] > 0.5
    y_test = target_columns['Anomaly']
    
    print(y_test.unique())

    df = pd.DataFrame(labled, columns = ['label'])
    print(df.label)
    
    outliers = df['label']
    outliers = outliers.fillna(0)
    print(outliers.unique())
    print(outliers)

    print(y_test)

    outliers = outliers.to_numpy()
    y_test = y_test.to_numpy()

    # get (mis)classification
    cf = confusion_matrix(outliers, y_test)

    # true/false positives/negatives
    print(cf)
    (tn, fp, fn, tp) = cf.flatten()

    print(f"""{cf}
    % of transactions labeled as fraud that were correct (precision): {tp}/({fp}+{tp}) = {tp/(fp+tp):.2%}
    % of fraudulent transactions were caught succesfully (recall):    {tp}/({fn}+{tp}) = {tp/(fn+tp):.2%}
    % of g-mean value : root of (specificity)*(recall) = ({tn}/({fp}+{tn})*{tp}/({fn}+{tp})) = {(tn/(fp+tn)*tp/(fn+tp))**0.5 :.2%}""")
    

    
    #save model in the DB
    # save the local file to mongodb
    with open(model_fpath, 'rb') as infile:
        file_id = fs.put(
                infile.read(), 
                model_name=model_name
                )
    # insert the model status info to ModelStatus collection 
    params = {
            'model_name': model_name,
            'file_id': file_id,
            'inserted_time': datetime.now()
            }
    result = collection_model.insert_one(params)
    client.close()

    print("hello OC_SVM")

def lstm_autoencoder():
    mongoClient = MongoClient()
    #host = Variable.get("MONGO_URL_SECRET")
    host = os.environ['MONGO_URL_SECRET'] 
    client = MongoClient(host)
    
    db_test = client['coops2022_aug']
    collection_aug=db_test['mongo_aug1']
    
    try:
        moldset_df = pd.DataFrame(list(collection_aug.find()))
        
    except:
        print("mongo connection failed")
        return False
    
    print(moldset_df)

    outlier = moldset_df[moldset_df.Class == 1]
    print(outlier.head())
    labled = pd.DataFrame(moldset_df, columns = ['Filling_Time','Plasticizing_Time','Cycle_Time','Cushion_Position','Class'])

    labled.columns = map(str.lower,labled.columns)
    labled.rename(columns={'class':'label'},inplace=True)
    print(labled.head()) 
    
    

    # splitting by class
    fraud = labled[labled.label == 1]
    clean = labled[labled.label == 0]

    print(f"""Shape of the datasets:
        clean (rows, cols) = {clean.shape}
        fraud (rows, cols) = {fraud.shape}""")
    
    # shuffle our training set
    clean = clean.sample(frac=1).reset_index(drop=True)

    # training set: exlusively non-fraud transactions
    global TRAINING_SAMPLE 
    if clean.shape[0] < TRAINING_SAMPLE:
        TRAINING_SAMPLE=(clean.shape[0]//5)*4

    X_train = clean.iloc[:TRAINING_SAMPLE].drop('label', axis=1)
    train = clean.iloc[:TRAINING_SAMPLE].drop('label', axis=1)

    # testing  set: the remaining non-fraud + all the fraud 
    X_test = clean.iloc[TRAINING_SAMPLE:].append(fraud).sample(frac=1)
    test = clean.iloc[TRAINING_SAMPLE:].append(fraud).sample(frac=1)
    test.drop('label', axis = 1, inplace = True)
    # 여기 test set이랑 train set 겹침

    print(f"""Our testing set is composed as follows:

            {X_test.label.value_counts()}""")
    
    X_test, y_test = X_test.drop('label', axis=1).values, X_test.label.values

    print(f"""Shape of the datasets:
        training (rows, cols) = {X_train.shape}
        Testing  (rows, cols) = {X_test.shape}""")

    with tf.device("/gpu:0"):

        # transforming data from the time domain to the frequency domain using fast Fourier transform
        train_fft = np.fft.fft(X_train)
        test_fft = np.fft.fft(X_test)

        scaler = MinMaxScaler()
        X_train = scaler.fit_transform(X_train)
        X_test = scaler.transform(X_test)
        scaler_filename = "scaler_data"

        # reshape inputs for LSTM [samples, timesteps, features]
        X_train = X_train.reshape(X_train.shape[0], 1, X_train.shape[1])
        print("Training data shape:", X_train.shape)
        X_test = X_test.reshape(X_test.shape[0], 1, X_test.shape[1])
        print("Test data shape:", X_test.shape)
       
        #scaler and lstm autoencoder model save
        db_model = client['coops2022_model']
        fs = gridfs.GridFS(db_model)
        collection_model=db_model['mongo_scaler_lstm']
       
        model_fpath = f'{scaler_filename}.joblib'
        joblib.dump(scaler, model_fpath)
        
        # save the local file to mongodb
        with open(model_fpath, 'rb') as infile:
            file_id = fs.put(
                    infile.read(), 
                    model_name=scaler_filename
                    )
        # insert the model status info to ModelStatus collection 
        params = {
                'model_name': scaler_filename,
                'file_id': file_id,
                'inserted_time': datetime.now()
                }
        result = collection_model.insert_one(params)


        # load the model
        collection_model=db_model['mongo_LSTM_autoencoder']
        
        model_name = 'LSTM_autoencoder'
        model_fpath = f'{model_name}.joblib'
        result = collection_model.find({"model_name": model_name}).sort([("uploadDate", -1)])
        print(result)
        cnt=result.count()
        print(cnt)
        print(result[0])
        print(result[cnt])
        if len(list(result.clone()))==0:
            print("empty")
            model = autoencoder_model(X_train)
        else:
            print("not empty")
            file_id = str(result[cnt]['file_id'])
            
            model = LoadModel(mongo_id=file_id).clf
        
        joblib.dump(model, model_fpath)
        
        model.compile(optimizer='adam', loss='mae')
        
        # 이상값은 -1으로 나타낸다.
        print(model.summary())

        nb_epochs = 100
        batch_size = 10
        history = model.fit(X_train, X_train, epochs=nb_epochs, batch_size=batch_size, validation_split=0.05).history

        X_pred = model.predict(X_train)
        X_pred = X_pred.reshape(X_pred.shape[0], X_pred.shape[2])
        X_pred = pd.DataFrame(X_pred, columns=train.columns)
        X_pred.index = train.index

        scored = pd.DataFrame(index=train.index)
        Xtrain = X_train.reshape(X_train.shape[0], X_train.shape[2])
        scored['Loss_mae'] = np.mean(np.abs(X_pred-Xtrain), axis = 1)

        plt.figure(figsize=(16,9), dpi=80)
        plt.title('Loss Distribution', fontsize=16)
        sns.distplot(scored['Loss_mae'], bins = 20, kde= True, color = 'blue');
        plt.xlim([0.0,.5])
        plt.show()


        # calculate the loss on the test set
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
    
    
        db_model = client['coops2022_model']
        fs = gridfs.GridFS(db_model)
        collection_model=db_model['mongo_LSTM_autoencoder']
       
        model_name = 'LSTM_autoencoder'
        model_fpath = f'{model_name}.joblib'
        joblib.dump(model, model_fpath)
        
        # save the local file to mongodb
        with open(model_fpath, 'rb') as infile:
            file_id = fs.put(
                    infile.read(), 
                    model_name=model_name
                    )
        # insert the model status info to ModelStatus collection 
        params = {
                'model_name': model_name,
                'file_id': file_id,
                'inserted_time': datetime.now()
                }
        result = collection_model.insert_one(params)
    client.close()

    print("hello auto encoder")


if __name__ == "__main__":
    print("entering main")
    
    if sys.argv[1] == 'iqr':
        print("entering iqr")
        iqr_mds_gan()
    elif sys.argv[1] == 'lstm':
        print("entering lstm")
        lstm_autoencoder()
    elif sys.argv[1] == 'oc_svm':
        print("entering svm")
        oc_svm()
    print("hello main")
 
