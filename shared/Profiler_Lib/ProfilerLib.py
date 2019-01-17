class Profiler():
    import pyspark
    from pyspark.sql import SQLContext
    import pandas as pd
    from pyspark.sql.functions import udf
        
    def __init__(self,data):
        import pandas as pd
        self.count=0
        self.type_dict={}
        self.dict_counter={}
        self.stat_dict_num={}
        self.stat_dict_cat={}
        self.data=data
        self.profiler=pd.DataFrame()
        self.cate_profiler=pd.DataFrame()
    def column_list(self):
        data=self.data
        import pandas as pd
        self.numeric_col=[i[0] for i in data.dtypes if i[1] != 'string']
        self.cat_col=[i[0] for i in data.dtypes if i[1] == 'string']
        self.del_cols=[]

        for i in self.numeric_col:
            distinct=data.select(i).distinct().count()
            if distinct <= 10:
                self.cat_col.append(i)
                self.del_cols.append(i)
        self.numeric_col=list(set(self.numeric_col)-set(self.del_cols))
    def get_null_values(self):
        import pandas as pd
        data=self.data
        profile=pd.DataFrame(columns=['column_name','null_count'])
        column_names=data.columns
        #global dict_counter
        null_count_list=[]
        for col in column_names:
            null_count=data.filter(data[col].isNull()).count()
            null_count_list.append(null_count)
            self.dict_counter[col]=null_count
        profile['column_name']=column_names
        profile['null_count']=null_count_list
    def get_type(self):
        import pandas as pd
        data=self.data
        #global type_dict
        for col,ty in data.dtypes:
            self.type_dict[str(col)]=str(ty)
        dataF=pd.DataFrame()
        dataF['name']=self.type_dict.keys()
        dataF['type']=self.type_dict.values()
    def get_stats(self):
        import pandas as pd
        data=self.data
        column_list=self.numeric_col
        for col in column_list:

            stat_dict=data.select(col).summary().rdd.collectAsMap()
            self.stat_dict_num[col]=stat_dict
    def cate_stats(self):
        import pandas as pd
        data=self.data
        columns=self.cat_col
        for col in columns:
            distinct=data.select(col).distinct().count()
            self.stat_dict_cat[col]=distinct
    def numerical_profiler(self):
        import pandas as pd
        numeric_col=self.numeric_col
        data=self.data
        for i in numeric_col:
            this_dict={'Column':[i],'type':[self.type_dict[i]],'Null_count':[self.dict_counter[i]],'%Null':["{0:.2f}".format((self.dict_counter[i]/data.count())*100)]}
            for j in self.stat_dict_num[i].items():
                this_dict[j[0]]=["{0:.2f}".format(float(j[1]))]
            this_dict['Range']=str(this_dict['min'])+'-'+str(this_dict['max'])
            _temp=pd.DataFrame.from_dict(this_dict)
            self.profiler=pd.concat([self.profiler,_temp],axis=0)
        self.profiler=self.profiler.set_index(['Column'])
    
        
    def cat_profiler(self):
        import pyspark
        import pandas as pd
        def nonasciitoascii(unicodestring):
        
            if unicodestring.isascii():
                count+=1
            return unicodestring.isascii()
        convertedudf = pyspark.sql.functions.udf(nonasciitoascii)
        cat_col=self.cat_col
        data=self.data
        count=0
        for i in cat_col:
            
            this_dict={'Column':[i],'type':[self.type_dict[i]],'Null_count':[self.dict_counter[i]],'%Null':[(self.dict_counter[i]/data.count())*100]}
            distict_count=data.select(i).distinct().count()
            this_dict['categories']=distict_count

            count_dict=data.cube(i).count()
            count_dict=count_dict.rdd.collectAsMap()
            if self.dict_counter[i]==0:
                del(count_dict[None])
            sorted_by_value = sorted(count_dict.items(), key=lambda kv: kv[1])
            string=''


            converted = data.select(i,convertedudf(pyspark.sql.functions.col(i)))
            this_dict['Non_ASCII']=count
            for idx,st in enumerate(sorted_by_value[-5:]):
                this_dict['Top_Feature_'+str(idx)]=str(st[0])+':'+str(st[1])
            #printtthis_dict_dict_dict_dict_dicts_dicts_dictis_dict)
            _temp=pd.DataFrame.from_dict(this_dict)
            self.cate_profiler=pd.concat([self.cate_profiler,_temp],axis=0,ignore_index=True,sort=False)
            count=0
        self.cate_profiler=self.cate_profiler.set_index('Column')
        
    def caller(self):
        self.column_list()
        self.get_null_values()
        self.get_type()
        self.get_stats()
        self.cate_stats()
        self.numerical_profiler()
        self.cat_profiler()
        return self.cate_profiler,self.profiler