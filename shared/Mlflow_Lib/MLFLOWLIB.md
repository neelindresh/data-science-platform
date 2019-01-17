# mlflowlib

The functions used in the mlflowlib library are:

### 1.  log_data
⦁	The log data function is used to get expName, paramDict and metricDict as input.
⦁	The the exp name is set in the mlflow.
⦁	The mlflow is started using start_run function.
⦁	The key,value in the paramDict is processed with logparam function.
⦁	Then the data is logged into the experiment.

### 2.  log_file
⦁	The log file funtion is used to get the expname, fileversion and filename as input.
⦁	The the exp name is set in the mlflow.
⦁	The mlflow is started using start_run function.
⦁	The logparam is used to return the Date-time, version and filename. 
⦁	Then the data is logged into the experiment.
