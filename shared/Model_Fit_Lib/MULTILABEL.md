# MultiLabel

BinaryRelevance class uses the following functions:

1.  __init__
⦁	The init constructor is the default function  used to get the input data as model, feature column,labelcolumn and pipline.

2.  fit
⦁	The fit function is used to fit the logistic regression model with the feature column, label column and the predicted column.
⦁	The output for predictionCol is named as _pred, the output for rawpredictionCol is named as _rawPrediction and the output for probabilityCol is named as prob.
⦁	This function is used to fit the train data and then append the model.It uses pipeline stages in model  to fit the train data.

3.  transform
⦁	The Transform function is used to transform the pipeline.It gets the models as input.
⦁	Then the model is fitted and tranformed to return the data_predicted value.

4.  save
⦁	The save function is used to save the path of the pipeline.

5.  load
⦁	The load function is used to load the pipeline as picklefile format.

6.  find_recommendation
⦁	The find recommendation function is used to get user and id column as input.
⦁	Then the predicted test data, label columns are converted into dictionary as dict_val and dict_pred , which is used to compare the test data id with the user data.
⦁	If they are equal, it selects the data and store into labelcolumns and predicted.
⦁	The product dict is used to provide the current and recommended product.
⦁	The dict_val and dict_pred items are zipped to return the current and recommended product as output.
