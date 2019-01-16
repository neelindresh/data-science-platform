from pyspark.ml.feature import VectorAssembler, OneHotEncoder, StringIndexer
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.ml import Transformer

class LabelEncode(Transformer, DefaultParamsReadable, DefaultParamsWritable):
    
    def __init__(self):
        super(LabelEncode, self).__init__()
        self.excludeCols = ['TARGET']        
    
    def _transform(self, df):
        independent_df = df.select(*list(set(df.columns)-set(self.excludeCols)))
        cc=[cat[0] for cat in independent_df.dtypes if cat[1]=='string']
        for column in cc:
#             print(column)
            sti=StringIndexer(inputCol=column,outputCol='index_'+column)
            df=sti.fit(df).transform(df)
            df=df.drop(column)
        
        print('Label Encoding completed.')
        
        return df
    

class OHEncode(Transformer, DefaultParamsReadable, DefaultParamsWritable):
    
    def __init__(self):
        super(OHEncode, self).__init__()
    
    def _transform(self, df):
        ohe_columns=[col for col in df.columns if col.startswith('index_')]
        ohe_columns=[col for col in ohe_columns if df.select(col).distinct().count()>2]
        for column in ohe_columns:
            sti=OneHotEncoder(inputCol=column,outputCol='ohe_'+column)
            df=sti.transform(df)
            df=df.drop(column)
        
        print('One-Hot Encoding completed.')
        
        return df

    
class VectorChange(Transformer, DefaultParamsReadable, DefaultParamsWritable):
    
    def __init__(self):
        super(VectorChange, self).__init__()
        self.excludeCols = ['TARGET']
        
    def _transform(self, df):       
        assem=VectorAssembler(inputCols=list(set(df.columns)-set(self.excludeCols)),outputCol='features')
        df=assem.transform(df)
        
        print('Vector Encoding completed.')
        
        return df