from pyspark.ml.pipeline import  Transformer,Estimator
from pyspark.ml.param.shared import HasInputCol,HasOutputCol,HasInputCols,HasOutputCols
from pyspark import keyword_only
from pyspark.sql.functions import udf
from pyspark.ml import Pipeline
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import Row
from pyspark.ml.feature import Imputer
from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.ml.feature import VectorAssembler,OneHotEncoder,StringIndexer

class LabelEncode(Transformer,HasInputCol,HasOutputCol,HasInputCols,HasOutputCols):
    @keyword_only
    def __init__(self,outputCols=None):
        super(LabelEncode,self).__init__()
        kwargs=self._input_kwargs
        self.setParams(**kwargs)
    @keyword_only
    def setParams(self,inputCol=None,outputCols=None):
        kwargs=self._input_kwargs
        return self._set(**kwargs)
    def _transform(self,df):
        label_col=self.getOutputCols()
        independent_df=df.select(*list(set(df.columns)-set(label_col)))
        cc=[cat[0] for cat in independent_df.dtypes if cat[1]=='string']
        for column in cc:
            print(column)
            sti=StringIndexer(inputCol=column,outputCol='index_'+column)
            df=sti.fit(df).transform(df)
            df=df.drop(column)
        return df
    
class OHEncode(Transformer,HasInputCol,HasOutputCol,HasOutputCols):
    @keyword_only
    def __init__(self):
        super(OHEncode,self).__init__()
        kwargs=self._input_kwargs
        self.setParams(**kwargs)
    @keyword_only
    def setParams(self,inputCol=None,outputCol=None):
        kwargs=self._input_kwargs
        return self._set(**kwargs)
    def _transform(self,df):
        ohe_columns=[col for col in df.columns if col.startswith('index_')]
        ohe_columns=[col for col in ohe_columns if df.select(col).distinct().count()>2]
        for column in ohe_columns:
            sti=OneHotEncoder(inputCol=column,outputCol='ohe_'+column)
            df=sti.transform(df)
            df=df.drop(column)
        #print(df.columns)
        #df=df.join(label_column)
        return df
    
class VectorChange(Transformer,HasInputCol,HasOutputCol,HasOutputCols):
    @keyword_only
    def __init__(self,outputCols=None):
        super(VectorChange,self).__init__()
        kwargs=self._input_kwargs
        self.setParams(**kwargs)
        
    @keyword_only
    def setParams(self,inputCol=None,outputCols=None):
        kwargs=self._input_kwargs
        return self._set(**kwargs)

    def _transform(self,df):
        target_col=self.getOutputCols()
        assem=VectorAssembler(inputCols=list(set(df.columns)-set(target_col)),outputCol='features')
        df=assem.transform(df)
        return df