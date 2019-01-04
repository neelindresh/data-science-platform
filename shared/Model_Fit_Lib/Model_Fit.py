from pyspark.ml.pipeline import  Transformer,Estimator
from pyspark.ml.param.shared import HasInputCol,HasOutputCol
from pyspark import keyword_only
from pyspark.sql.functions import udf

class ModelFit(Transformer,HasInputCol,HasOutputCol):
    @keyword_only
    def __init__(self,inputCol=None):
        super(ModelFit,self).__init__()
        self.model=None
        kwargs=self._input_kwargs
        self.setParams(**kwargs)
    @keyword_only
    def setParams(self,inputCol=None,outputCol=None):
        kwargs=self._input_kwargs
        return self._set(**kwargs)
    def logistic_regression(self,data):
        from pyspark.ml.classification import LogisticRegression
        lr=LogisticRegression(featuresCol='features',labelCol='TARGET')
        self.model=lr.fit(self.train_data)
    def random_forest(self,data):
        from pyspark.ml.classification import RandomForestClassifier
        lr=RandomForestClassifier(featuresCol='features',labelCol='TARGET')
        self.model=lr.fit(self.train_data)
    def split_train_test(self,data):
        train_data,test_data=data.randomSplit([0.7,0.3])
        self.train_data=train_data
        self.test_data=test_data
    def _get_feature_list(self):
        return self.df.columns
    def _get_train_data(self):
        return self.train_data
    def _get_test_data(self):
        return self.test_data
    def _get_model(self):
        return self.model
    def _evaluate_prediction(self):
        from pyspark.ml.evaluation import BinaryClassificationEvaluator
        predicted=self.model.transform(self.test_data)
        binary_eval=BinaryClassificationEvaluator(labelCol='TARGET')
        print(binary_eval.evaluate(predicted))
    def _get_predicted(self):
        predicted=self.model.transform(self.test_data)
        return predicted
    def _transform(self,df):
        self.df=df
        self.split_train_test(df)
        if self.getInputCol() =='LR':
            self.logistic_regression(df)
        elif self.getInputCol() =='RF':
            self.random_forest(df)
        return self.model