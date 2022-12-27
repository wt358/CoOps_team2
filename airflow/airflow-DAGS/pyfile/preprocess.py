import pandas as pd
import numpy as np
import time

from sklearn.preprocessing import StandardScaler
from sklearn.manifold import MDS

from sklearn.cluster import DBSCAN

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

def IQR(section):
    
    section.iloc[:,6:17]=section.iloc[:,6:17].apply(pd.to_numeric,errors='coerce')
    for i in range(6,17):
        level_1q = section.iloc[:,i].quantile(0.025)
        level_3q = section.iloc[:,i].quantile(0.975)
        IQR = level_3q - level_1q
        rev_range = 1.5 # 제거 범위 조절 변수
        section = section[(section.iloc[:,i] <= level_3q + (rev_range * IQR)) & (section.iloc[:,i] >= level_1q - (rev_range * IQR))] ## sectiond에 저장된 데이터 프레임의 이상치 제거 작업
    return section

def MDS_molding(pds):
    list1=[]## 불량여부가 라벨링된 구간별 데이터 프레임을 저장할 리스트

    for i in range(len(pds)):
        start_time = time.time()
        print('%d 번째' %i)
        pds[i]['TimeStamp']=pd.to_datetime(pds[i]['TimeStamp']).astype('int64')/10**9
        dataframe=pds[i].iloc[:,5:]
        if len(pds[i])>=30:
            std = StandardScaler().fit_transform(pds[i].iloc[:,5:]) ## 정규화 진행
            end_time = time.time()
            print('    if std 코드 실행 시간: %20ds' % (end_time - start_time))
            mds_results = MDS(n_components=2).fit_transform(std) ## mds차원축소결과 저장(시간이 좀 많이 소요됨)
            end_time = time.time()
            print('    if mds 코드 실행 시간: %20ds' % (end_time - start_time))
            mds_results=pd.DataFrame(mds_results) ##dataframe 형태로 저장 
            end_time = time.time()
            print('    if df 코드 실행 시간: %20ds' % (end_time - start_time))
            list1.append(customize(pds[i],mds_results))## 구간별 라벨링 데이터 프레임을 리스트에 저장
            end_time = time.time()
            print('    if 코드 실행 시간: %20ds' % (end_time - start_time))
        else :
            answer=pd.DataFrame(np.zeros(len(pds[i]))) 
            answer.rename(columns = {0:'Class'},inplace=True) 
            dataframe.reset_index(inplace=True,drop=True)     
            result = pd.DataFrame(pd.concat([pds[i].iloc[:,5:],answer], axis = 1))
            list1.append(result)
            end_time = time.time()
            print('    else 코드 실행 시간: %20ds' % (end_time - start_time))
    df_all=pd.concat(list1, ignore_index=True)
    
    return df_all