from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
class BinaryRelevance():
    def __init__(self,featuresCol):
        self.models=[]
        self.featuresCol=featuresCol
        self.data=None
        self.label_columns=None
        self.pipeline=None
    def fit(self,train,columns):
        self.label_columns=columns
        for i in columns:
            print(i)
            lr=LogisticRegression(featuresCol=self.featuresCol,labelCol=i,predictionCol=i+'_pred',rawPredictionCol=i+'_rawPrediction',probabilityCol=i+'prob')
            model=lr.fit(train)
            self.models.append(model)
        self.pipeline=Pipeline(stages=self.models)
        self.pipeline.fit(train)
        #print(self.models)
    def transform(self,data):
        '''
        for model in self.models:
            data=model.transform(data)
        '''
        
        
        from pyspark.ml import Pipeline
        if self.pipeline is None:
            pipe=Pipeline(stages=self.models)
            self.pipeline=pipe
        else:
            pipe=self.pipeline
        data_predicted=pipe.fit(data).transform(data)
        self.data=data_predicted
        return data_predicted
    def save(self,path):
        self.pipeline.save(path)
    def load(self,path):
        self.pipeline=Pipeline.load('pipeline.pkl')
    def find_recommendation(self,user,id_column):
        test=self.data
        predicted=[i for i in test.columns if i.endswith('pred')]
        label_columns=self.label_columns
        dict_val=test.where(test[id_column]==user).select(*label_columns).toPandas().to_dict()
        dict_pred=test.where(test[id_column]==user).select(*predicted).toPandas().to_dict()
        product_dict={'current':[],'recommendation':[]}
        for i in zip(dict_val.items(),dict_pred.items()):
            #print(i)
            if int(i[1][1][0])==1 and int(i[0][1][0])==0:
                product_dict['recommendation'].append(i[0][0])
            if int(i[0][1][0])==1:
                product_dict['current'].append(i[0][0])

        return product_dict