'''
This library caters to the functionality required for API deployment
'''

def post_predictions(query):
    ''' 
    This function generates predictions based on the input query to a specific classifier.
    Args: query(dict) 
    Output: prediction(array)    
    '''
    
    from sklearn.externals import joblib
    from valueLib import valueDict    
    
    # Initialize array
    predictions = []
    
    # Load the classifier
    classifier = joblib.load(valueDict['testModelPath'])
    
    # Process the query and generate predictions
    for item in query:
        text = item['text']
        category = classifier.predict([text])[0]
        predictions.append({"category": category, "text": text})
    
    # Return predictions
    return predictions

def post_homeLoanDefault_predictions(Path):
    '''
    This function generates predictions based on the input data to a home loan default classifier.
    Args: Path(dict)
    Output: prediction(array)
    '''
    
    from pyspark.sql import SparkSession
    from pyspark.ml.classification import LogisticRegressionModel
    
    for item in Path:
        modelPath = item['modelPath']
        dataPath = item['dataPath']
    
    spark = SparkSession.builder.appName('HomeCredit').getOrCreate()
    data = spark.read.parquet(dataPath)
    
    #loading Model
    mm = LogisticRegressionModel.load(modelPath)
    
    #calculate predictions
    predicted = mm.transform(data)
    
    predictList = predicted.select('prediction').collect()
    predictList = [int(i.prediction) for i in predictList]
    
    return predictList
    

def api_run(swaggerSpecFile, appPort):
    '''
    This function deploys API on specified port with the provided swagger specification 
        
    Args: swaggerSpecFile (string: path for the file)
        appPort (Integer)
    Output: None
    '''    
    
    import connexion
    
    # Initialize API based on swagger spec file
    app1 = connexion.App(__name__)
    app1.add_api(swaggerSpecFile)
    
    # Deploy API on the port
    app1.run(port=appPort)

# End-of-library