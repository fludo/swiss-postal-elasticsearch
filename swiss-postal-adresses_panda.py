import csv
from numpy.core.numeric import NaN
import pandas as pd
import numpy as np
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk
from elasticsearch.helpers import parallel_bulk

from collections import deque

# In memory processing, warning, this requires a lot of RAM
data = pd.read_csv("Post_Adressdaten20200804.csv", low_memory=False, sep=";",names=np.arange(16),encoding = "ISO-8859-1")
INDEX="dataframe03"
TYPE= "record"
def rec_to_es(df):
    import json
    for record in df.to_dict(orient="record"):
        yield (json.dumps(record, default=int))


#TODO use convert dict https://www.geeksforgeeks.org/change-data-type-for-one-or-more-columns-in-pandas-dataframe/
#col_dict = {
#    'ONRP' : int,
#    'BFSNR' : int,
#    'STR_ID' : int
#}

list0 = [0]
df1 = data[data[0]==1]
df1 = df1.drop(list0,1)
df1.columns = ['ONRP','BFSNR','PLZ_TYP','PLZ','PLZ_ZZ','GPLZ','ORT_BEZ_18','ORT_BEZ_27','KANTON','SPRACHCODE','SPACHCODE_ABW','BREIFZ_DURCH','GILT_AB_DAT','PLZ_BRIEFZUST','PLZ_COFF']
#df1 = df1.astype(col_dict)
df1['ONRP'] = df1['ONRP'].astype(int)
df1['BFSNR'] = df1['BFSNR'].astype(int)
df2 = data[data[0]==2]
df2 = df2.drop(list0+list(range(7,16)),1)
df2.columns = ['ONRP','LAUFNUMMER','BEZ_TYP','SPACHCODE','ORT_BEZ_18','ORT_BEZ_27']
df3 = data[data[0]==3].drop(list0+list(range(5,16)),1)
df3.columns = ['BFSNR','GEMEINDENAME','KANTON','AGGLONR']
df3['BFSNR'] = df3['BFSNR'].astype(int)
df4 = data[data[0]==4]
df4 = df4.drop(list0+list(range(11,16)),1)
df4.columns = ['STR_ID','ONRP','STR_BEZ_K','STR_BEZ_L','STR_BEZ_2K','STR_LOK_TYP','STR_BEZ_SPC','STR_BEZ_COFF','STR_GANZFACH','STR_FACH_ONRP']
df4['ONRP'] = df4['ONRP'].astype(int)
df4['STR_ID'] = df4['STR_ID'].astype(int)
df5 = data[data[0]==5]
df6 = data[data[0]==6].drop(list0+list(range(8,16)),1)
df6.columns = ['HAUSKEY','STR_ID','HNR','HNR_A','HNR_COFF','GANZFACH','FACH_ONRP']
df6['STR_ID'] = df6['STR_ID'].astype(int)
df7 = data[data[0]==7]
df8 = data[data[0]==8]
# NEW_GEB_COM N/A dans la version gratuite
#df12 = data[data[0]==12]

# print each table
#print(data)
#print(df1)
#print(df2)
#print(df3)
#print(df4)
#print(df5)
#print(df6)
#print(df7)
#print(df8)

#print(df12)

print("Merging......")
# build our denormalized DataFrame
# https://stackoverflow.com/questions/30584486/join-two-pandas-dataframe-using-a-specific-column
plzstr = pd.merge(df1,df4,on='ONRP', how='right',suffixes=('','_y'))
plzstr.drop(list(plzstr.filter(regex='_y$')),axis=1,inplace=True)
plzstr2 = pd.merge(plzstr,df6,on='STR_ID', how='right',suffixes=('','_y'))
plzstr2.drop(list(plzstr2.filter(regex='_y$')),axis=1,inplace=True)
plzstr3 = pd.merge(plzstr2,df3,on='BFSNR', how='left',suffixes=('','_y'))
plzstr3.drop(list(plzstr3.filter(regex='_y$')),axis=1,inplace=True)

# live check, search using panda
#pd.set_option('display.max_columns', 500)
#print(plzstr2.loc[plzstr2['GPLZ'].isin(['1000']) & plzstr2['HNR'].isin(['4']) ])



#plzstr3.to_csv("dataframe.csv",index=False)


es = Elasticsearch([
    {'host':'localhost',
    'port': 9200,
    }],http_auth=('favrel','elastic')
)


if not es.indices.exists(INDEX):
    raise RuntimeError('index does not exists, use `curl -X PUT "localhost:9200/%s"` and try again'%INDEX)
plzstr3= plzstr3.fillna("").astype(str)

# print first 10 entries
#print(list(rec_to_es(plzstr3.head(10))))

# see https://github.com/elastic/elasticsearch-py/blob/master/examples/bulk-ingest/bulk-ingest.py
#r = streaming_bulk(client=es,index=INDEX, actions=rec_to_es(plzstr3)) # return a dict
r = parallel_bulk(client=es,index=INDEX, actions=rec_to_es(plzstr3), 
                       chunk_size=500, thread_count=8, queue_size=8)


deque(r, maxlen=0)
print(not r["errors"])
print(r)
