# DataCleaningLib
DataCleaningLib class uses the following functions:

1. fill_na_numerical
The FILL NA NUMERICAL is used to fill the null values of the numerical data.
It  imputes the data based on the mean value and then its fit  and transform the model.

2. get_max_value
The GET MAX VALUE function is used to find the maximum value of each category.
 It then returns a tuple containing the maximum value.
 
3. fill_na_categorical
The FILL NA CATEGORICAL function is used to find null values  of the categorical data.
It fills  the null data  using the get max value function  and return result.

4. find_null_values
The FIND NULL VALUES function is used to find all the missing value.
It gets data,column,count as input.
It drops the null columns that is above 30 percent in the data.
It process and find the null count (i.e)less than 30 NA.
It finally returns the dropped column list i.e less than 30 NA.

5. clean_string
The CLEAN STRING function is use to clean the string having null values in the data.
It gets the data and checks each column as string and  then return the data.

6. type_cast
The TYPECAST function is used to convert the string datatype into float and  returns the data.

7. cleaning
        The CLEANING function gets the data of 30 percent ratio as input.
        This is the main function, it performs the above tasks.
First, the CLEAN STRING is used to find the string and cleans it.
The FIND NULL VALUES is used to find the missig values and drop the highest frequency of occuring null values.
The categorical and numerical data is splitted in the dataset.
Now the categorical null colums are imputed using the FILL NA CATEGORICAL function with the highest frequency of values.(i.e  max )
Similarly,the numerical null colums are imputed using the FILL NA NUMERICAL function with the highest frequency of values.(i.e mean)
Finally, It returns the list of null columns dropped and cleaned.


