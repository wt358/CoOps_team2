from datetime import timedelta
from datetime import datetime
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
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

class buidGAN():
    def __init__(self, out_shape, num_classes):
        self.latent_dim = 32
        self.out_shape = out_shape
        self.num_classes = num_classes
        self.clip_value = 0.01
        optimizer = Adam(0.0002, 0.5)
        #optimizer = RMSprop(lr=0.00005)

        # build discriminator
        self.discriminator = self.build_discriminator()
        self.discriminator.compile(loss=['binary_crossentropy'],
        optimizer=optimizer,
        metrics=['accuracy'])

        # build generator
        self.generator = self.build_generator()

        # generating new data samples
        noise = Input(shape=(self.latent_dim,))
        label = Input(shape=(1,))
        gen_samples = self.generator([noise, label])

        self.discriminator.trainable = False

        # passing gen samples through disc. 
        valid = self.discriminator([gen_samples, label])

        # combining both models
        self.combined = Model([noise, label], valid)
        self.combined.compile(loss=['binary_crossentropy'],
                optimizer=optimizer,
                metrics=['accuracy'])
        self.combined.summary()

    def wasserstein_loss(self, y_true, y_pred):
        return K.mean(y_true * y_pred)

    def build_generator(self):
        init = RandomNormal(mean=0.0, stddev=0.02)
        model = Sequential()
       
        model.add(Dense(128, input_dim=self.latent_dim))
        #model.add(Dropout(0.2))
        model.add(LeakyReLU(alpha=0.2))
        model.add(BatchNormalization(momentum=0.8))

        model.add(Dense(256))
        #model.add(Dropout(0.2))
        model.add(LeakyReLU(alpha=0.2))
        model.add(BatchNormalization(momentum=0.8))

        model.add(Dense(512))
        #model.add(Dropout(0.2))
        model.add(LeakyReLU(alpha=0.2))
        model.add(BatchNormalization(momentum=0.8))

        model.add(Dense(self.out_shape, activation='tanh'))
        model.summary()

        noise = Input(shape=(self.latent_dim,))
        label = Input(shape=(1,), dtype='int32')
        label_embedding = Flatten()(Embedding(self.num_classes, self.latent_dim)(label))
                                                                                                                                                                        
        model_input = multiply([noise, label_embedding])
        gen_sample = model(model_input)

        return Model([noise, label], gen_sample, name="Generator")

    def build_discriminator(self):
        init = RandomNormal(mean=0.0, stddev=0.02)
        model = Sequential()
        
        model.add(Dense(512, input_dim=self.out_shape, kernel_initializer=init))
        model.add(LeakyReLU(alpha=0.2))
                                                
        model.add(Dense(256, kernel_initializer=init))
        model.add(LeakyReLU(alpha=0.2))
        model.add(Dropout(0.4))
                                                                                
        model.add(Dense(128, kernel_initializer=init))
        model.add(LeakyReLU(alpha=0.2))
        model.add(Dropout(0.4))
        
        model.add(Dense(1, activation='sigmoid'))
        model.summary()
        
        gen_sample = Input(shape=(self.out_shape,))
        label = Input(shape=(1,), dtype='int32')
        label_embedding = Flatten()(Embedding(self.num_classes, self.out_shape)(label))

        model_input = multiply([gen_sample, label_embedding])
        validity = model(model_input)

        return Model(inputs=[gen_sample, label], outputs=validity, name="Discriminator")

    def train(self, X_train, y_train, pos_index, neg_index, epochs, batch_size=32, sample_interval=50):

        # Adversarial ground truths
        valid = np.ones((batch_size, 1))
        fake = np.zeros((batch_size, 1))

        for epoch in range(epochs):
         
            #  Train Discriminator with 8 sample from postivite class and rest with negative class
            idx1 = np.random.choice(pos_index, 8)
            idx0 = np.random.choice(neg_index, batch_size-8)
            idx = np.concatenate((idx1, idx0))
            samples, labels = X_train[idx], y_train[idx]
            samples, labels = shuffle(samples, labels)
            # Sample noise as generator input
            noise = np.random.normal(0, 1, (batch_size, self.latent_dim))

            # Generate a half batch of new images
            gen_samples = self.generator.predict([noise, labels])

            # label smoothing
            if epoch < epochs//1.5:
                valid_smooth = (valid+0.1)-(np.random.random(valid.shape)*0.1)
                fake_smooth = (fake-0.1)+(np.random.random(fake.shape)*0.1)
            else:
                valid_smooth = valid 
                fake_smooth = fake
            
            # Train the discriminator
            self.discriminator.trainable = True
            d_loss_real = self.discriminator.train_on_batch([samples, labels], valid_smooth)
            d_loss_fake = self.discriminator.train_on_batch([gen_samples, labels], fake_smooth)
            d_loss = 0.5 * np.add(d_loss_real, d_loss_fake)

            # Train Generator
            # Condition on labels
            self.discriminator.trainable = False
            sampled_labels = np.random.randint(0, 2, batch_size).reshape(-1, 1)
            # Train the generator
            g_loss = self.combined.train_on_batch([noise, sampled_labels], valid)

            # Plot the progress
            if (epoch+1)%sample_interval==0:
                print (f"{epoch} [D loss: {d_loss[0]}, acc.: {100*d_loss[1]}] [G loss: {g_loss}]")
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

    df.drop(columns={'_id',
        },inplace=True)

    df=df['idx', 'Machine_Name','TimeStamp','Additional_Info_1', 'Additional_Info_2',
            'Average_Back_Pressure', 'Average_Screw_RPM', 'Clamp_Close_Time',
            'Clamp_Open_Position', 'Cushion_Position', 'Cycle_Time', 'Filling_Time',
            'Injection_Time', 'Plasticizing_Position',
            'Plasticizing_Time', 'Shot_Number', 'Switch_Over_Position',
            ]
    #IQR
    print(df)
    print(df.dtypes)
    
    df=df.reset_index(drop=True)
    section=df
    section.iloc[:,5:17]=section.iloc[:,5:17].apply(pd.to_numeric,errors='coerce')
    print(df.dtypes)
    print(df)
    for i in range(5,17):
        level_1q = section.iloc[:,i].quantile(0.025)
        level_3q = section.iloc[:,i].quantile(0.975)
        IQR = level_3q - level_1q
        rev_range = 1.5 # 제거 범위 조절 변수
        section = section[(section.iloc[:,i] <= level_3q + (rev_range * IQR)) & (section.iloc[:,i] >= level_1q - (rev_range * IQR))] ## sectiond에 저장된 데이터 프레임의 이상치 제거 작업
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


    print(df_all)
    print(df_all.columns)

    #GAN

    df=df_all
    df['Class'] = df_all['Class'].map(lambda x: 1 if x == -1 else 0)

    print(df)
    print(df['Class'].value_counts(normalize=True)*100)

    print(f"Number of Null values: {df.isnull().any().sum()}")

    print(f"Dataset has {df.duplicated().sum()} duplicate rows")

    df.drop_duplicates(inplace=True)
    try:
        df.drop(columns={'Labeling'}
                ,inplace=True)
    except:
        print("passed")
    

    print(df)

    # checking skewness of other columns

    print(df.drop('Class',1).skew())
    
    skew_cols = df.drop('Class', 1).skew().loc[lambda x: x>2].index
    print(skew_cols)

    print(device_lib.list_local_devices())
    print(tf.config.list_physical_devices())


    for col in skew_cols:
        lower_lim = abs(df[col].min())
        normal_col = df[col].apply(lambda x: np.log10(x+lower_lim+1))
        print(f"Skew value of {col} after log transform: {normal_col.skew()}")
    
    scaler = StandardScaler()
    #scaler = MinMaxScaler()
    X = scaler.fit_transform(df.drop('Class', 1))
    y = df['Class'].values
    print(X.shape, y.shape)

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, stratify=y)


    gan = buidGAN(out_shape=X_train.shape[1], num_classes=2)
    # cgan.out_shape=X_train.shape[1]

    y_train = y_train.reshape(-1,1)
    pos_index = np.where(y_train==1)[0]
    neg_index = np.where(y_train==0)[0]
    gan.train(X_train, y_train, pos_index, neg_index, epochs=50)#원래 epochs= 5000

    print(df.shape)
    print(X_train.shape)
    
    noise = np.random.normal(0, 1, (1225, 32))
    sampled_labels = np.ones(1225).reshape(-1, 1)

    gen_samples = gan.generator.predict([noise, sampled_labels])
    gen_samples = scaler.inverse_transform(gen_samples)
    print(gen_samples.shape)

    gen_df = pd.DataFrame(data = gen_samples,
            columns = df.drop('Class',1).columns)
    gen_df['Class'] = 1
    print(gen_df)

    Class0 = df[df['Class'] == 0 ]
    print(Class0)
    
    pca = PCA(n_components = 2)
    PC = pca.fit_transform(gen_df)
    PCdf = pca.fit_transform(Class0)

    VarRatio = pca.explained_variance_ratio_
    VarRatio = pd.DataFrame(np.round_(VarRatio,3))

    CumVarRatio    = np.cumsum(pca.explained_variance_ratio_)
    CumVarRatio_df = pd.DataFrame(np.round_(CumVarRatio,3))

    Result = pd.concat([VarRatio , CumVarRatio_df], axis=1)
    print(pd.DataFrame(Result))
    print(pd.DataFrame(PC))

    pca3 = PCA(n_components = 3)
    PC3 = pca3.fit_transform(gen_df)
    PC_df = pca3.fit_transform(Class0)

    VarRatio3 = pca3.explained_variance_ratio_
    VarRatio3 = pd.DataFrame(np.round_(VarRatio3,3))

    CumVarRatio3    = np.cumsum(pca3.explained_variance_ratio_)
    CumVarRatio_df3 = pd.DataFrame(np.round_(CumVarRatio3,3))

    Result3 = pd.concat([VarRatio3 , CumVarRatio_df3], axis=1)
    print(pd.DataFrame(Result3))
    print(pd.DataFrame(PC3))

    augdata = pd.concat([pd.DataFrame(Class0), gen_df])
    Augdata = augdata.reset_index(drop=True)
    print(Augdata)
    print(Augdata['Class'].value_counts(normalize=True)*100)
    
    mongoClient = MongoClient()
    host = Variable.get("MONGO_URL_SECRET")
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
    print("hello")


#provide the aug data that saved in the local to the aug topic in the kafka cluster
def oc_svm():
    
    mongoClient = MongoClient()
    host = Variable.get("MONGO_URL_SECRET")
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
    result = collection_model.find({"model_name": model_name}).sort('uploadDate', -1)
    print(result)
    print(result[0])
    if len(list(result.clone()))==0:
        print("empty")
        model = OneClassSVM(kernel = 'rbf', gamma = 0.001, nu = 0.04).fit(target_columns)
    else:
        print("not empty")
        file_id = str(result[0]['file_id'])

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

    print("hello OC_SVM")

def lstm_autoencoder():
    mongoClient = MongoClient()
    host = Variable.get("MONGO_URL_SECRET")
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
    X_train = clean.iloc[:TRAINING_SAMPLE].drop('label', axis=1)
    train = clean.iloc[:TRAINING_SAMPLE].drop('label', axis=1)

    # testing  set: the remaining non-fraud + all the fraud 
    X_test = clean.iloc[:TRAINING_SAMPLE].append(fraud).sample(frac=1)
    test = clean.iloc[:TRAINING_SAMPLE].append(fraud).sample(frac=1)
    test.drop('label', axis = 1, inplace = True)
    # 여기 test set이랑 train set 겹침

    print(f"""Our testing set is composed as follows:

            {X_test.label.value_counts()}""")
    
    X_test, y_test = X_test.drop('label', axis=1).values, X_test.label.values

    print(f"""Shape of the datasets:
        training (rows, cols) = {X_train.shape}
        Testing  (rows, cols) = {X_test.shape}""")


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
    result = collection_model.find({"model_name": model_name}).sort('uploadDate', -1)
    print(result)
    print(result[0])
    if len(list(result.clone()))==0:
        print("empty")
        model = autoencoder_model(X_train)
    else:
        print("not empty")
        file_id = str(result[0]['file_id'])
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

    print("hello auto encoder")


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
            task_id="iqr_mds_gan",
            python_callable=iqr_mds_gan,
            depends_on_past=True,
            owner="coops2",
            retries=0,
            retry_delay=timedelta(minutes=1),
            )

    t2 = PythonOperator(
            task_id="OC_SVM",
            python_callable=oc_svm,
            depends_on_past=True,
            owner="coops2",
            retries=0,
            retry_delay=timedelta(minutes=1),
            )
    t3 = PythonOperator(
            task_id="LSTM_AUTO_ENCODER",
            python_callable=lstm_autoencoder,
            depends_on_past=True,
            owner="coops2",
            retries=0,
            )
    # 테스크 순서를 정합니다.
    # t1 실행 후 t2를 실행합니다.
    t1 >> t2
    t1 >> t3
