# ModelFit

ModelFit class uses the following functions:

1.  __init__ 
⦁	The init constructor is the default function  used to get the input using outputcolumns from the data.

2.  setParams
⦁	The set params function is the default function  used to get the input using outputcolumns from the data.

3.  logistic_regression
⦁	The Logistic regression is used to describe data and to explain the relationship between one dependent binary variable and one or more nominal, ordinal, interval or ratio-level independent variables. 
⦁	In this,First logistic regression classifier is imported.Then logistic model is called which has features columns and label columns as input.Finally, the train data is fitted into the model.

4.  random_forest
⦁	The Random forest builds multiple decision trees and merges them together to get a more accurate and stable prediction. 
⦁	In this,First random forest classifier is imported.Then randomforest model is called which has features columns and label columns as input.Finally, the train data is fitted into the model.

5.  split_train_test
⦁	The split train test function is used to split the data into the train data 70 percent and test data 30 percent.

6.  _get_feature_list
⦁	The get feature list is used to return the feature columns.

7.  _get_train_data
⦁	The get train data is used to return the train data.

8.  _get_test_data
⦁	The get test data is used to return the test data.

9.  _get_model
⦁	The get test model is used to return the model.

10. _evaluate_prediction
⦁	The evaluate prediction uses Binary Classification evaluator.The model is transformed with the test data.The binary evaluator use input labelcolumn as target.Then it returns the predicted value.

11. _get_predicted
⦁	The get predicted function is used to transform the model of test data and  then store it in  predicted.

12.  _transform
⦁	The tranform  function is used to get the dataframe as input.If the input value is LR it calls the logistic regression function. Whereas if the input value is RF it calls the random forest function.It returns the model as output.
