
# coding: utf-8

# In[1]:


from graphframes import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

import datetime



APP_NAME = 'bitcoin_1'





def logger(message):
    recent_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print('[%s] %s' % (recent_time, message))


def getvertex(txin_df, txout_df):
    #vertex
    df2 = txin_df.select([c for c in txin_df.columns if c in {'address'}])
    df3 = txout_df.select([c for c in txout_df.columns if c in {'address'}])
    #print(df2.count())
    #print(df3.count())
    
    df4 = df2.union(df3)
    #print(df4.count())
    
    vertex = df4.distinct() #get distinct row
    vertex = vertex.selectExpr("address as id")
    print(vertex.take(1)) #print out the dataset
#     print(vertex.count())
    return vertex



def getedge(txin_df, txout_df):
    #edge
    # 1527724800 : 2018-05-31 utc
    # 1535673600 : 2018-08-31 utc
    txin_df.createOrReplaceTempView("txintable")
    txout_df.createOrReplaceTempView("txouttable")
    edge = spark.sql("SELECT A1.address AS src, A2.address as dst, 1 as relationship, A1.txid                   from txintable as A1, txouttable as A2                  where A1.txid = A2.txid and A1.blocktime >= '1528588800' and A1.blocktime <= '1528675200' and A2.blocktime >= '1528588800' and A2.blocktime <= '1528675200' and (A1.address in ('1NDyJtNTjmwk5xPNhjgAMu4HDHigtobu1s','1N52wHoVR79PMDishab2XmRHsbekCdGquK','18x5Wo3FLQN4t1DLZgV2MoAMWXmCYL9b7M','1G47mSr3oANXMafVrR8UC4pzV7FEAzo3r9','1LAnF8h3qMGx3TSwNUHVneBZUEpwE4gu3D','1FoWyxwPXuj4C6abqwhjDWdz6D4PZgYRjA','1PFtrRjbq4aLfM7k4tyLZ3ZAuTsgLr6Q8Q','37Tm3Qz8Zw2VJrheUUhArDAoq58S6YrS3g','1P3rU1Nk1pmc2BiWC8dEy9bZa1ZbMp5jfg','168o1kqNquEJeR9vosUB5fw4eAwcVAgh8P') or A2.address in ('1NDyJtNTjmwk5xPNhjgAMu4HDHigtobu1s','1N52wHoVR79PMDishab2XmRHsbekCdGquK','18x5Wo3FLQN4t1DLZgV2MoAMWXmCYL9b7M','1G47mSr3oANXMafVrR8UC4pzV7FEAzo3r9','1LAnF8h3qMGx3TSwNUHVneBZUEpwE4gu3D','1FoWyxwPXuj4C6abqwhjDWdz6D4PZgYRjA','1PFtrRjbq4aLfM7k4tyLZ3ZAuTsgLr6Q8Q','37Tm3Qz8Zw2VJrheUUhArDAoq58S6YrS3g','1P3rU1Nk1pmc2BiWC8dEy9bZa1ZbMp5jfg','168o1kqNquEJeR9vosUB5fw4eAwcVAgh8P') )")
    #edge.collect()
    print(edge.take(1))
#     print(edge.count())
    return edge



#def wtriteVertexCsv(vertex):
#    vertex.write.format("com.databricks.spark.csv").option("header", "true").save("/home/joadmin/bitcoin-tx/vertex.csv")



#def readVertexCsv():  #??
#    vertex2 = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('/home/joadmin/bitcoin-tx/vertex.csv')
#    return vertex2


def getIndegree(g):
    #inDegrees
    
    # Query: Get in-degree of each vertex.
    #g.inDegrees.show()
    #print (type(g.inDegrees))
    indegrees_list = g.inDegrees.orderBy(g.inDegrees.inDegree.desc()).limit(10).collect()
    print(indegrees_list[0:10])
    
    #g.edges.filter("src = '1FhWNT1oJjdNVszHZCsWT8jHDJZkBDafei'").count()
    #g.edges.filter("txid = '002e2e3d9edb03e2f33ca5e9f48acbe68a87fc2cdc5ffd062424f46fc6bbea46'").count()
    return indegrees_list

def getOutdegree(g):
    #outDegrees
    outdegrees_list = g.outDegrees.orderBy(g.outDegrees.outDegree.desc()).limit(10).collect()
    print(outdegrees_list[0:10])
    return outdegrees_list

def getDegree(g):
    degrees_list = g.degrees.orderBy(g.degrees.degree.desc()).limit(10).collect()
    print(degrees_list[0:10])
    return degrees_list

def getPageRank(g):
    # Run PageRank algorithm, and show results.
    results = g.pageRank(resetProbability=0.15, maxIter=1)
    #results.vertices.select("id", "pagerank").show()
    pagerank_df = results.vertices.select("id", "pagerank")
    pagerank_df1 = pagerank_df.sort(['pagerank'], ascending=[0])
    #print(pagerank_df1.show())
    #show max pagerank
    print(pagerank_df1.first())
    #show top 10 pagerank
#     print(pagerank_df1.limit(10).collect())
    return pagerank_df1


# if __name__ == '__main__':

#     spark = SparkSession.builder.appName(APP_NAME).getOrCreate() #gcp
#     #spark = SparkSession.builder.appName(APP_NAME).config("spark.jars.packages", "graphframes:graphframes:0.5.0-spark2.1-s_2.11").getOrCreate() #local
    
#     logger('Reading blockchain data...')
#     txincsvpath = 'gs://bucket-1-btc/*-txIn.csv'
#     #txincsvpath = 'gs://bucket-1-btc/blocks-525308-526478-txIn.csv'
#     #txincsvpath = "/home/joadmin/bitcoin-tx/txIn/*.csv"  #local
#     txin_df = spark.read.load(txincsvpath,format="csv", delimiter="\t", header=True)
#     txoutcsvpath = 'gs://bucket-1-btc/*-txOut.csv'
#     #txoutcsvpath = 'gs://bucket-1-btc/blocks-525308-526478-txOut.csv'
#     #txoutcsvpath = '/home/joadmin/bitcoin-tx/txOut/*.csv'  #local
#     txout_df = spark.read.load(txoutcsvpath,format="csv", delimiter="\t", header=True)
#     #print(type(txout_df), txout_df.printSchema())
#     print("txin count:",txin_df.count())
#     print("txout count:",txout_df.count())

#     logger('get vertex...')
#     vertex = getvertex(txin_df, txout_df)

#    wtriteVertexCsv(vertex)
#    vertex2 = readVertexCsv()  #??
#    print(vertex2.count())     #??


#     logger('get edge...')
#     edge = getedge(txin_df, txout_df)

#     logger('create GraphFram...')
#     g = GraphFrame(vertex, edge)

#     logger('get indegree...')
#     indegrees_list = getIndegree(g)
#     print(type(indegrees_list))

#     logger('get outdegree...')
#     outdegrees_list = getOutdegree(g)
#     print(type(outdegrees_list))
    

#     logger('get degree...')
#     degrees_list = getOutdegree(g)
#     print(type(degrees_list))
    

#     logger('get pageRank...')
#     pagerank_df1 = getPageRank(g)
#     print(type(pagerank_df1))


#     spark.stop()

    logger('Done!')


# In[2]:


spark = SparkSession.builder.appName(APP_NAME).getOrCreate() #gcp
#spark = SparkSession.builder.appName(APP_NAME).config("spark.jars.packages", "graphframes:graphframes:0.5.0-spark2.1-s_2.11").getOrCreate() #local

logger('Reading blockchain data...')
txincsvpath = 'gs://bucket-1-btc/*-txIn.csv'
#txincsvpath = 'gs://bucket-1-btc/blocks-525308-526478-txIn.csv'
#txincsvpath = "/home/joadmin/bitcoin-tx/txIn/*.csv"  #local
txin_df = spark.read.load(txincsvpath,format="csv", delimiter="\t", header=True)
txoutcsvpath = 'gs://bucket-1-btc/*-txOut.csv'
#txoutcsvpath = 'gs://bucket-1-btc/blocks-525308-526478-txOut.csv'
#txoutcsvpath = '/home/joadmin/bitcoin-tx/txOut/*.csv'  #local
txout_df = spark.read.load(txoutcsvpath,format="csv", delimiter="\t", header=True)
#print(type(txout_df), txout_df.printSchema())
print("txin count:",txin_df.count())
print("txout count:",txout_df.count())


# In[6]:


df_cur = spark.read.load('gs://btc-analysis-tw/data/Gemini_BTCUSD_d.csv', format="csv", delimiter=",", header=True)
df_cur.head()


# In[31]:


df_cur = df_cur.withColumn('Bounce', df_cur.Close - df_cur.Open)
df_cur = df_cur.withColumn('MaxBounce', df_cur.High - df_cur.Low)
df_cur.head()


# In[32]:


pd_cur = df_cur.toPandas()


# In[33]:


pd_cur = pd_cur.loc[(pd_cur["Date"] >= "2018-05-31") & (pd_cur["Date"] <= "2018-08-31")].sort_values(by=['Date'])


# In[34]:


pd_cur.plot(x='Date', y='Bounce', kind='bar', color='green', figsize=(40, 5))


# In[35]:


pd_cur.plot(x='Date', y='MaxBounce', kind='bar', color='green', figsize=(40, 5))


# In[36]:


txin_df.head()


# In[3]:


# spark = SparkSession.builder.appName(APP_NAME).config("spark.jars.packages", "graphframes:graphframes:0.5.0-spark2.1-s_2.11").getOrCreate() #local
# vertex = getvertex(txin_df, txout_df)
edge = getedge(txin_df, txout_df)
edge = edge.dropna()

# g = GraphFrame(vertex, edge)

# logger('get indegree...')
# indegrees_list = getIndegree(g)
# print(type(indegrees_list))


# In[40]:


edge.head()


# In[6]:


logger('get outdegree...')
outdegrees_list = getOutdegree(g)
print(type(outdegrees_list))


# In[32]:


logger('get pageRank...')
pagerank_df1 = getPageRank(g)
print(type(pagerank_df1))


# In[48]:


edge.take(5)


# In[56]:



max_in_nodes = ["1NDyJtNTjmwk5xPNhjgAMu4HDHigtobu1s","1N52wHoVR79PMDishab2XmRHsbekCdGquK","18x5Wo3FLQN4t1DLZgV2MoAMWXmCYL9b7M","1G47mSr3oANXMafVrR8UC4pzV7FEAzo3r9","1LAnF8h3qMGx3TSwNUHVneBZUEpwE4gu3D","1FoWyxwPXuj4C6abqwhjDWdz6D4PZgYRjA","1PFtrRjbq4aLfM7k4tyLZ3ZAuTsgLr6Q8Q","37Tm3Qz8Zw2VJrheUUhArDAoq58S6YrS3g","1P3rU1Nk1pmc2BiWC8dEy9bZa1ZbMp5jfg","168o1kqNquEJeR9vosUB5fw4eAwcVAgh8P"]
max_out_node = ["1NDyJtNTjmwk5xPNhjgAMu4HDHigtobu1s","3L86WSsX94pirYw81zYjL8ii3hMRNRuYhQ","3LaNNTg87XjTtXAqs55WV5DyWASEZizCXZ","1GX28yLjVWux7ws4UQ9FB4MnLH4UKTPK2z","3FxUA8godrRmxgUaPv71b3XCUxcoCLtUx2","3422VtS7UtCvXYxoXMVp6eZupR252z85oC","1LAnF8h3qMGx3TSwNUHVneBZUEpwE4gu3D","1CK6KHY6MHgYvmRQ4PAafKYDrg1ejbH1cE","1FoWyxwPXuj4C6abqwhjDWdz6D4PZgYRjA","1PFtrRjbq4aLfM7k4tyLZ3ZAuTsgLr6Q8Q"]

edge.filter(edge.src.isin(max_in_nodes)).count()


# In[5]:


import pandas as pd
pd_edge = edge.toPandas()
pd_edge.head()


# In[4]:


pd_edge_one = pd_edge[(pd_edge["src"] == "37Tm3Qz8Zw2VJrheUUhArDAoq58S6YrS3g") | (pd_edge["dst"] == "37Tm3Qz8Zw2VJrheUUhArDAoq58S6YrS3g")]


# In[23]:


max_in_nodes = ['1NDyJtNTjmwk5xPNhjgAMu4HDHigtobu1s','1N52wHoVR79PMDishab2XmRHsbekCdGquK','18x5Wo3FLQN4t1DLZgV2MoAMWXmCYL9b7M','1G47mSr3oANXMafVrR8UC4pzV7FEAzo3r9','1LAnF8h3qMGx3TSwNUHVneBZUEpwE4gu3D','1FoWyxwPXuj4C6abqwhjDWdz6D4PZgYRjA','1PFtrRjbq4aLfM7k4tyLZ3ZAuTsgLr6Q8Q','37Tm3Qz8Zw2VJrheUUhArDAoq58S6YrS3g','1P3rU1Nk1pmc2BiWC8dEy9bZa1ZbMp5jfg','168o1kqNquEJeR9vosUB5fw4eAwcVAgh8P']

pd_edge_ten = pd_edge[pd_edge["dst"].isin(max_in_nodes)]
pd_edge_ten['src'].astype(str).str[4:]
pd_edge_ten["src_4"] = pd_edge_ten.src.str[:4]
pd_edge_ten["dst_4"] = pd_edge_ten.dst.str[:4]
pd_edge_ten


# In[13]:


pd_edge_ten.shape


# In[46]:


import pyspark.sql.functions as F

df_grouped = edge.groupby('src').agg(F.collect_list(F.col("dst")).alias("dsts"))


# In[29]:


ranks = df_grouped.rdd.map(lambda r: 1.0)


# In[47]:


list(zip(pd_edge.src, pd_edge.dst))


# In[1]:



aa = list(zip(pd_edge.src, pd_edge.dst))[0:1000]

len(pd_edge[0:1000]["src"].unique())


# In[24]:


import networkx as nx
import matplotlib.pyplot as plt


plt.figure(figsize = (24, 16))
graph = nx.DiGraph()

graph.add_edges_from(list(zip(pd_edge_ten.src_4, pd_edge_ten.dst_4))) # (类似{“1”“2”，“权重”})

pos = nx.nx.spring_layout(graph, center=(4,5))
nx.draw_networkx_nodes(graph, pos, node_size=10, node_color="r")
edge_labels=nx.get_edge_attributes(graph,'weight')

nx.draw_networkx_labels(graph, pos, font_size=10, font_family="sans-serif", horizontalalignment="left")
nx.draw_networkx_edges(graph, pos, alpha=0.5)
nx.draw_networkx_edge_labels(graph,pos,edge_labels=edge_labels,label_pos=0.3)

plt.axis('off')
plt.show()

# plt.savefig("weighted_graph.png")
# plt.figure(figsize = (100, 50))
# plt
# plt.figure.Figure(figsize = (100, 50))


# In[15]:


import numpy as np
import networkx as nx
import matplotlib.pyplot as plt

A = np.matrix([[1,1],[2,1]])
G = nx.from_numpy_matrix(A)
nx.draw(G)
plt.show()
plt

