from pyspark.ml.param.shared import HasOutputCol
from pyspark import keyword_only
from pyspark.ml.feature import VectorAssembler,OneHotEncoder,StringIndexer

from pyspark.ml.util import JavaMLReadable, JavaMLWritable
from pyspark.ml.wrapper import JavaTransformer

class LabelEncode(JavaTransformer, JavaMLReadable, JavaMLWritable, HasOutputCol):
    
    @keyword_only
    def __init__(self, outputCol=None):
        super(LabelEncode, self).__init__()
        self.setOutputCol(outputCol)
    
    def _transform(self, df):
        label_col=self.getOutputCol()
        independent_df=df.select(*list(set(df.columns)-set(label_col)))
        cc=[cat[0] for cat in independent_df.dtypes if cat[1]=='string']
        for column in cc:
#             print(column)
            sti=StringIndexer(inputCol=column,outputCol='index_'+column)
            df=sti.fit(df).transform(df)
            df=df.drop(column)
        
        print('Label Encoding completed.')
        
        return df
    
class OHEncode(JavaTransformer, JavaMLReadable, JavaMLWritable):
    
    @keyword_only
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
    
class VectorChange(JavaTransformer, JavaMLReadable, JavaMLWritable, HasOutputCol):
    
    @keyword_only
    def __init__(self, outputCol=None):
        super(VectorChange, self).__init__()
        self.setOutputCol(outputCol)

    def _transform(self, df):
        target_col=self.getOutputCol()       
        assem=VectorAssembler(inputCols=list(set(df.columns)-set(target_col)),outputCol='features')
        df=assem.transform(df)
        
        print('Vector Encoding completed.')
        
        return df