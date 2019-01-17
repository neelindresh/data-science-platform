# apilib

The functions used in the apilib library are:

### 1.  post_predictions
⦁	The post prediction function is used to predict the api deployment based on the input query to a specific classifier.
⦁	It loads the testmodel path into classifier and the results is appended as predictions for category and text.
⦁	Then returns the predictions.

### 2.  post_homeLoanDefault_predictions
⦁	The post home loan default prediction function is used to generate predictions based on the input data.
⦁	First, the modelpath and the datapath s inputted.
⦁	Then Homecredit  data file in parquet format is loaded.
⦁	Now,the logisticregression model is loaded with the model path.
⦁	Then the model is transormed  and stored as predicted list.

### 3.  api_run
·	The api run function is used to get swagger spec file and app port as input.
·	The swagger spec file represents the definitions of the model and data path.
·	The app port represents the port number of api deployment.
·	First, the connexion is imported and intialized.
·	The swagger spec file is added into the api.
·	The API is deployed by the run  function,which inputs the app port.
·	The output of api deployment is done in the port  http://0.0.0.0:8000/ .
