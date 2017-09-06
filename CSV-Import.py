
# coding: utf-8

# In[1]:

import redis;
import pandas;


# In[16]:

def readCSVFile(file):
    data=pandas.read_csv(file,"|",header=0, na_values='?', skipinitialspace=True,);
    return data;
    pass;

def loadDataToRedis(redis_cli,schema_name,dataframe,id_col_name):
    df=dataframe;
    name=None;
    row_count=df.shape[0];
    columns=df.columns;
    if(id_col_name not in columns):
        print("Column "+id_col_name+ " doesnot exits");
    for i in range(row_count):
        row=df.ix[i];
        id_val=str(row[id_col_name]).strip();
        name=schema_name+":"+str(id_val);        
        redis_cli.sadd(schema_name+"_set",id_val);
        for c in columns:        
            redis_cli.hset(name=name,key=c,value=str(row[c]).strip());                   
    pass;


# In[3]:

r_cli=redis.Redis(host="10.100.100.200",port=6379,db=1);


# In[6]:

#r_cli.hset(name="user:key1",key="name",value="abcde");
#r_cli.hset(name="user:key1",key="add",value="pune");


# In[17]:

dir=""
filename=dir+"dummy_data.csv";
df=readCSVFile(filename);
print(df.shape);


# In[18]:

loadDataToRedis(r_cli,'login',df,'node_ipaddr')
print("Successfully loaded");


# In[11]:




# In[ ]:




# In[ ]:



