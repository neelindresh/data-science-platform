# EncodeLib
The functions used in the encode library are:


### LabelEncode class 

__init__
    The init constructor is the default function  used to get the input using outputcolumns from the data.
    
setParams
    The set params function is the default function  used to get the input using outputcolumns from the data.
    
_transform
     The transform function is used to take label column using get output  columns.  Then the independent columns are selected by subtracting the data columns with label columns. Now each independent  columns are checked with string type. Then the independent columns which  are string are encoded using the String Indexer with the output columns renamed as index_.Then the stringindexer is fitted , transformed  and return the dataframe.
                
### OHEncode class

__init__
    The init constructor is the default function  used to get the input using outputcolumns from the data.
    
setParams
    The set params function is the default function  used to get the input using outputcolumns from the data.
    
_transform
    The transform function is used to take the Label encoded data and finds the distinct columns having more than two category with the output column is renamed as ohe_.Then the onehot encoding is fitted , transformed  and return the dataframe.
                
### VectorChange class
 
__init__
    The init constructor is the default function  used to get the input using outputcolumns from the data.
    
setParams
    The set params function is the default function  used to get the input using outputcolumns from the data.
    
_transform
    The transform function is used to take the input as features which is obtained by subtracting the data columns and the target columns with the output column is renamed as features.Then the Vector assembler is transformed and return the output as vector.




