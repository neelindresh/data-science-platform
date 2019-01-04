from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import udf
from catboost import CatBoostClassifier
import numpy as np
from sklearn.ensemble import RandomForestClassifier
import matplotlib.pyplot as plt
import pandas as pd
import lightgbm as lgb
import warnings
warnings.filterwarnings("ignore")


class Feature_Importance_Transformer():
    def __init__(self,data):
        self.data=data
        
    def dataset(self,data,y=np.zeros(1),X=np.zeros(1)):
        if len(self.data)>50000:
            self.data=self.data.iloc[:50000]
        if len(y)>50000:
            y=y[:50000]
        if len(X)>50000:
            X=X[:50000]
        return self.data,y,X
    
    def sort_features(self,feature_importance,column_name):
        '''
        Sort the feature importance values 
        args:
        1.feature_importance: <NPARRAY> containing the feature importance 
        2. column names: <LIST> containing the column names from the dataframe
        returns: sorted DICT of {key: column_name ,value :importance}
        '''
        feature_map={}
        for feature in zip(feature_importance,column_name):
            feature_map[feature[1]]=feature[0]
        sorted_feature_map=sorted(feature_map.items(), key=lambda x: x[1],reverse=True)
        return dict(sorted_feature_map)
    
    def plot_barh(self,sorted_feature_map,n_features,figsize):
        plt.figure(figsize=figsize)
        #plt.figure().suptitle('Top',n_features+'Features')
        plt.barh(list(sorted_feature_map.keys())[:n_features],list(sorted_feature_map.values())[:n_features])
        plt.show()

    def feature_importance_lightGBM(self,y,categorical_columns,n_features=None,learning_rate=0.1,n_estimators=100,num_iterations=100,num_leaves=31,tree_learner='serial',num_threads=0,device_type='cpu',seed=None,boosting='gbdt',objective='regression',task='train',config='',min_data_in_leaf=20,max_depth=-1,bagging_fraction=1.0):
        '''
        FIND FEATURE IMPORTANCE USING LIGHT GBM
        args:
        1.data:<SPARK DATAFRAME> Actual Dataframe
        2.categorical_columns:<LIST> of categorical columns
        3.n_features:<INT> number of features we want to plot
        RETURNS : <DICT> of top N_features
        '''
        data,y,x=self.dataset(self.data,y)
        
        if n_features is None:
            n_features=data.shape[1]
            
        for col in categorical_columns:
            data[col]=data[col].astype('category')
            
        d_train = lgb.Dataset(data, label=y)
        param = {'learning_rate' : learning_rate, 'n_estimators': n_estimators,'num_iterations':num_iterations,'num_leaves':num_leaves,
                 'tree_learner':tree_learner,'num_threads':num_threads,'device_type':device_type,'seed':seed,'boosting':boosting,
                 'objective':objective,'task':task,'config':config,
                 'min_data_in_leaf':min_data_in_leaf,'max_depth':max_depth,'bagging_fraction':bagging_fraction}

        model2 = lgb.train(params=param,train_set=d_train,categorical_feature=list(categorical_columns))
        print('Plot Top',n_features,'feature importances…')
        ax = lgb.plot_importance(model2, max_num_features=n_features,figsize=(16,16))
        plt.show()
        #return model2        
        
    def find_features_RandomForest(self,X,y,n_features=None,n_estimators=100, class_weight=None, criterion='gini',
            max_depth=None, max_features='auto', max_leaf_nodes=None, min_samples_leaf=1,
            min_samples_split=2, min_weight_fraction_leaf=0.0,
            n_jobs=-1, oob_score=False, random_state=0,
            verbose=0,figsize=(16,12)):
        '''
        FIND FEATURE IMPORTANCE USING RANDOM FOREST
        args:
        1. X :<NPARRAY> containing the independent variables
        2. y :<NPARRAY> containing the dependent variables
        3. n_features: <INT> number of features we want to plot

        '''
        data,y,X=self.dataset(self.data,y=y,X=X)
        if n_features is None:
            n_features=X.shape[1]
       
        rf=RandomForestClassifier(n_estimators=n_estimators, class_weight=class_weight, criterion=criterion,
            max_depth=max_depth, max_features=max_features, max_leaf_nodes=max_leaf_nodes, min_samples_leaf=min_samples_leaf,
            min_samples_split=min_samples_split, min_weight_fraction_leaf=min_weight_fraction_leaf,
            n_jobs=n_jobs, oob_score=oob_score, random_state=random_state,
            verbose=0)
        rf.fit(X,y)
        sorted_list=self.sort_features(rf.feature_importances_,numeric_columns)
        print('Plot Top',n_features,'feature importances…')
        self.plot_barh(sorted_list,n_features,figsize)

    
    def feature_importance_CATBOOST(self, y,categorical_columns,n_features=20,n_estimators=100,iterations=None,learning_rate=0.1,depth=None,l2_leaf_reg=None,loss_function='Logloss',border_count=None,thread_count=None,verbose=0,figsize=(16,8)):
        
        '''
        FIND FEATURE IMPORTANCE USING catBoost
        args:
        1. data_pandas :<pandas dataframe> containing the independent variables
        2. y :<PANDAS SERIES> containing the dependent variables
        3. categorical_columns: <LIST> of categorical columns
        '''
        data,y,X=self.dataset(self.data,y=y)
        category_index_list=[list(data.columns).index(cat) for cat in categorical_columns]
        cb=CatBoostClassifier(n_estimators=n_estimators,iterations=iterations,learning_rate=learning_rate,depth=depth,l2_leaf_reg=l2_leaf_reg,loss_function=loss_function,border_count=border_count,thread_count=thread_count)
        cb.fit(data,y,cat_features=category_index_list,verbose=verbose)
        sorted_map=self.sort_features(cb.feature_importances_,cb.feature_names_)
        print('Plot Top',n_features,'feature importances…')
        self.plot_barh(sorted_map,n_features,figsize)
        
