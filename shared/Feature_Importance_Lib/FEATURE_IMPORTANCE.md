# Feature_Importance_Transformer

Feature_Importance_Transformer class uses the following functions:

1. __init__
⦁	The init constructor is the default function  used to get the input from the data.

2. dataset
⦁	The dataset function is used to limit the data to 50,000 thousand records.

3. sort_features
⦁	The sort features function is used to sort the features based on the high correlation values.
⦁	It gets feature importance and column as input.
⦁	It sorts column named sorted_feature_map with its importance as result.

4. plot_barh
⦁	The plot barh is used to plot a bar graph for the n features.
⦁	It gets sorted_feature_map and n_features as input.
⦁	The output is plotted for the n_features.

5. feature_importance_lightGBM
⦁	The feature importance lightGBM function is used to return the top correlated n features used for model building.
⦁	It is  used to handle categorical columns.
⦁	It gets data,categorical columns,n_features,learning_rate and n_estimators as input.
⦁	The lightGBM model is trained based on the train data and categorical column.
⦁	It plot importance based on the model as output.

6. find_features_RandomForest
⦁	It is used to return the top correlated n features used for model building
⦁	It is  used to handle categorical columns.
⦁	It gets n_features and n_estimators as input.
⦁	It uses random forest classifier to  fit the model.
⦁	The sort features used to find  feature importance and plot graph.

7. feature_importance_CATBOOST
⦁	It is used to return the top correlated n features used for model building.
⦁	This algorithm is used to handle categorical (CAT) data automatically.
⦁	It  gets  n_features, n_estimator and categorical columns as input.
⦁	It uses CatBoost Classifier to fit the model.
⦁	The sort features used to find  feature importance and plot graph.
