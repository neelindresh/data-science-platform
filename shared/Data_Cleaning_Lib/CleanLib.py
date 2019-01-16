from pyspark.sql.functions import *
from pyspark.ml.feature import Imputer
from pyspark.ml import Transformer
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable


class DataCleaningLib(Transformer, DefaultParamsReadable, DefaultParamsWritable):    
    
    def __init__(self):
        '''
        Initialize instance of this class
        '''
        super(DataCleaningLib, self).__init__()
    
    def fill_na_numerical(self,data,columns):
        '''
        FILL NULL VALUES FOR NUMERICAL DATA
        args:
        1.data: <SPARK DATAFRAME> actual spark dataframe
        2.columns: <LIST> of numerical columns we want to Impute

        return: <SPARK DATAFRAME>Imputed spark dataframe
        '''
        columns=list(columns)
        imputer=Imputer(inputCols=columns,outputCols=['imputed_'+str(col) for col in columns])
        dataCopy=imputer.fit(data).transform(data)
        return dataCopy    
    
    def get_max_value(self,count_dict):
        '''
        GET MAX VALUE FROM A DICTIONARY
        args:
        1.count_dict: <DICT> frequency of each category {key=category name, value=count}
        return: a tuple containing _key with tha maximim value _max
        '''
        _max=0
        _key=0
        for k,v in count_dict.items():
            if v>_max:
                _key=k
                _max=v
        return (_key,_max)
    
    
    def fill_na_categorical(self,data,columns,count_list):
        '''
        FILL NULL VALUES FOR CATEGORICAL DATA
        args:
        1.data: <SPARK DATAFRAME> actual spark dataframe
        2.columns: <LIST> of categorical columns we want to Impute

        return: <SPARK DATAFRAME>Imputed spark dataframe
        '''
        for category in columns:
#             print('Imputing-->',category)
            count_dict=data.cube(category).count()
            count_dict=count_dict.rdd.collectAsMap()
            key,_=self.get_max_value(count_dict)
#             print('NULL value Imputed with:',key,' for',category)
            data=data.fillna(key,subset=[category])
        return data
    
   
    def find_null_values(self,data,column_names,ratio=0.5):
        '''
        FIND ALL NULL VALUES IN DATASET
        args:
        1. data: Actual Spark < DataFrame >
        2. column_names: < LIST > of columns to find missing values
        3. total_count: < int > row count
        4. ration: < float > range(0-1) how much null values is acceptable
        return: < dict > {key=column name over having more than acceptable null values, value= total number of null values}
        '''
        less_than_thirty_NA={}
        total_count=data.count()
        drop_column_list={}
#         print('REMOVING columns with NULL value above:',total_count*ratio,ratio*100,'%')
#         print('----------------------------------------------------------')
        for col in column_names:
#             print('processing-->',col)
            null_count=data.filter(data[col].isNull()).count()

            if null_count>(total_count*ratio) and null_count>0:
#                 print(col,' having NULL VALUES -->',null_count)
                drop_column_list[col]=null_count
            elif null_count<(total_count*ratio) and null_count>0:
                less_than_thirty_NA[col]=null_count
#         print('!-----DONE------!')
        return drop_column_list,less_than_thirty_NA
    
    
    def clean_string(self,data):
        '''
        CLEANS THE STRING HAVING NULL VALUES IN DATASET
        args:
        1.data: <SPARK DATAFRAME> actual spark dataframe
        return: <SPARK DATAFRAME>Cleaned spark dataframe
        '''
        for col in data.dtypes:
            #data=data.withColumn(colName=col,col=when(data[col] == ' NA' ,None).otherwise(data[col]))
            if col[1] =='string':
                data=data.withColumn(col[0],regexp_replace(col[0], ' ', ''))
                data=data.withColumn(colName=col[0],col=when(data[col[0]] == 'NA' ,lit(None)).otherwise(data[col[0]]))
        return data
    
    
    def type_cast(self,data):
        for col in data.dtypes:
            if col[1] == 'string':
                if data.select(col[0]).limit(1).toPandas().to_dict()[col[0]][0].strip().isdigit():
#                     print(col[0])
                    data = data.withColumn(col[0], data[col[0]].cast("float"))
        return data
    
    
    def _transform(self, data, ratio=0.3):
        '''
        CLEANING DATASET
        args:
        1.data: <SPARK DATAFRAME> actual spark dataframe
        2.ratio: < float > range(0-1) how much null values is acceptable
        3.columns: <LIST> of columns we want to clean
        return: <SPARK DATAFRAME>Imputed spark dataframe
        '''
        print('Number of columns in raw dataset : ',len(data.columns))        
        
        print('1.Data Cleaning and Preprocessing')
        self.data=data
        column_names=data.columns
        data=self.clean_string(data)
        drop_column_list,null_list_ratio=self.find_null_values(data,column_names,ratio)
#         print(null_list_ratio)
        data=data.drop(*drop_column_list)   
        
        categorial_columns=[cat[0] for cat in data.dtypes if cat[1]=='string']
#         print(categorial_columns)
        numerical_columns=set(data.columns)-set(categorial_columns)
#         print(categorial_columns)
        null_cate=set(null_list_ratio.keys())-set(numerical_columns)
        null_num=set(null_list_ratio.keys())-set(null_cate)
        
        print('2.IMPUTING CATEGORICAL VALUES')
       
        data=self.fill_na_categorical(data,null_cate,null_list_ratio)
        print('3.IMPUTING NUMERICAL VALUES')
       
        data=self.fill_na_numerical(data,null_num)
        data=data.drop(*list(null_num))
        
        print('Number of columns in processed dataset : ',len(data.columns))
        
        return data