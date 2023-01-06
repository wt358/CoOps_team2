#!/usr/bin/env python
# coding: utf-8


import logging
import os
import re
import datetime
import pandas as pd
import numpy as np
from collections import Counter
from joblib import dump, load

import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap

from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.preprocessing import StandardScaler
from sklearn.datasets import make_moons, make_circles, make_classification
from sklearn.neural_network import MLPClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.svm import SVC
from sklearn.gaussian_process import GaussianProcessClassifier
from sklearn.gaussian_process.kernels import RBF, DotProduct
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier, AdaBoostClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn.discriminant_analysis import QuadraticDiscriminantAnalysis

import random
from logger import *

######################## Model Parameters ########################
knn_params = {
    'n_neighbors' : list(np.arange(1,20, step=4)),
    'weights' : ["uniform", "distance"],
    'metric' : ['euclidean', 'manhattan', 'minkowski']
}

svc_params = {'C': [0.1, 1, 10],
             'gamma': [0.1, 1, 10] }



decision_parmas = {'max_features': ['auto', 'sqrt', 'log2'],
              'ccp_alpha': [0.1, .01, .001],
              'max_depth' : [4,6,8],
              'criterion' :['gini', 'entropy']
             }

random_params = { 
    'n_estimators': [200, 400],
    'max_features': ['auto', 'sqrt', 'log2'],
    'max_depth' : [4,6,8],
    'criterion' :['gini', 'entropy']
}

nn_params = {
    'solver': ['lbfgs'], 
#     'max_iter': [500, 1000, 1500], 
#     'alpha': 10.0 ** -np.arange(1, 10, step=3), 
    'hidden_layer_sizes':[5,10,15], 
}


ada_params = {
    'n_estimators':[10, 50, 100],
    'learning_rate':[0.001, 0.01, 0.1]
}

gnb_params = {
    'var_smoothing': [1e-2, 1e-5, 1e-10, 1e-15]
}

qda_params = {
    'reg_param': [0.1, 0.2, 0.3, 0.4, 0.5]
}


##################################################################
if __name__ == '__main__':
    random.seed(42)
    
    logging.info('########## START DATA EVALUATION ##########')
    logging.info('########## Read & Preprocess data ##########')
    
    eval_dt = datetime.datetime.now().strftime("%Y-%m-%d")
    
    """수정사항: 데이터 Read 부분: 주생산품 데이터 with augmentation"""
    df = pd.read_csv('./data/raw/1.csv', encoding='utf-8-sig', index_col=0)

    df = df.drop(['Machine_Name','Additional_Info_1', 'Additional_Info_2','Shot_Number'],axis=1)
    idx = df.pop('idx')
    y = df.pop('Class')
    dt = pd.to_datetime(df.pop('TimeStamp'))

    usecols = df.columns
    df_ = df[usecols].copy()

    scaler = StandardScaler()
    X = scaler.fit_transform(df_)

    logging.info('########## Train & Evaluate data ##########')
    """수정사항: 예외처리 -> train_data에 라벨이 1가지인 경우(불량이 없는 경우) -> 학습 불가능"""
    try:
        y.value_counts().shape[0] == 2
    except:
        logging.info('########## Cannot find anomalies ##########')
            
    names = [
        "Nearest Neighbors",
        "RBF SVM",
        # "Gaussian Process",
        "Decision Tree",
        "Random Forest",
        "Neural Net",
        "AdaBoost",
        "Naive Bayes",
        "QDA",
    ]

    classifiers = [
        KNeighborsClassifier(),
        SVC(),
        # GaussianProcessClassifier(),
        DecisionTreeClassifier(),
        RandomForestClassifier(),
        MLPClassifier(alpha=1, max_iter=3000),
        AdaBoostClassifier(),
        GaussianNB(),
        QuadraticDiscriminantAnalysis(),
    ]
    params = [knn_params,
            svc_params,
            # gaussian_params,
            decision_parmas,
            random_params,
            nn_params,
            ada_params,
            gnb_params,
            qda_params]


    best_score = {}
    best_param = {}
    learning_time = []
    for name, clf, param in zip(names, classifiers, params):
        start = datetime.datetime.now()
        clf_gridsearch = GridSearchCV(clf, param, scoring='f1')
        clf_gridsearch.fit(X, y)
        best_score[name] = clf_gridsearch.best_score_
        best_param[name] = clf_gridsearch.best_params_

        end = datetime.datetime.now() - start
        learning_time.append([name, start, end, end.total_seconds()])
        logging.info('Gridsearch time for {}: {} sec'.format(name, end.total_seconds()))


    logging.info('########## Save models & parameters ##########')
    os.makedirs('./data/result/{}'.format(train_dt), exist_ok=True)
    os.makedirs('./data/model/{}'.format(train_dt), exist_ok=True)
    
    for name, clf in zip(names, classifiers):
        clf_best = clf.set_params(**best_param[name])
        clf_best.fit(X, y)
        dump(clf, './model/{}/{}.model'.format(re.sub(" ","_",name)))
        
    best_score.to_csv('./data/result/{}/best_score.csv'.format(eval_dt) ,encoding='utf-8-sig')
    best_param.to_csv('./data/result/{}/best_param.csv'.format(eval_dt) ,encoding='utf-8-sig')
    pd.DataFrame(learning_time, 
                 columns = ['name','start','end','time']
                ).to_csv('./data/result/{}/learning_time.csv'.format(eval_dt) ,encoding='utf-8-sig')





