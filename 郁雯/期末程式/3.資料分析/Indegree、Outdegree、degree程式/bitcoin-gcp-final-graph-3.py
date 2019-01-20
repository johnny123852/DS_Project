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
    print(vertex.count())
    return vertex



def getedge(txin_df, txout_df):
    #edge
    txin_df.createOrReplaceTempView("txintable")
    txout_df.createOrReplaceTempView("txouttable")
    edge_havena = spark.sql("SELECT A1.address AS src, A2.address as dst, 1 as relationship1, A1.txid  \
                 from txintable as A1, txouttable as A2 \
                 where A1.txid = A2.txid")
    #edge.collect()
    #edge_havena = edge_havena.dropna()  #176273863 
    edge_havena.createOrReplaceTempView("edgehavenatable")
    edge = spark.sql("SELECT src, dst, sum(relationship1) as relationship, txid  \
                 from edgehavenatable \
                 where dst IS NOT NULL \
                 group by src, dst, txid")
    
    print(edge.take(1))
    print(edge.count())
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
    results = g.pageRank(resetProbability=0.15, maxIter=10)
    #results.vertices.select("id", "pagerank").show()
    pagerank_df = results.vertices.select("id", "pagerank")
    pagerank_df1 = pagerank_df.sort(['pagerank'], ascending=[0])
    #print(pagerank_df1.show())
    #show max pagerank
    print(pagerank_df1.first())
    #show top 10 pagerank
    print(pagerank_df1.limit(10).collect())
    return pagerank_df1


if __name__ == '__main__':

    spark = SparkSession.builder.appName(APP_NAME).getOrCreate() #gcp
    #spark = SparkSession.builder.appName(APP_NAME).config("spark.jars.packages", "graphframes:graphframes:0.5.0-spark2.1-s_2.11").getOrCreate() #local
    
    logger('Reading blockchain data...')
    txincsvpath = 'gs://bucket-1-btc/*-txIn.csv'
    #txincsvpath = 'gs://bucket-1-btc/block-ALL-In-1/blocks-531163-532333-txIn.csv'
    #txincsvpath = "/home/joadmin/bitcoin-tx/txIn/*.csv"  #local
    txin_df = spark.read.load(txincsvpath,format="csv", delimiter="\t", header=True)
    txoutcsvpath = 'gs://bucket-1-btc/*-txOut.csv'
    #txoutcsvpath = 'gs://bucket-1-btc/block-ALL-Out-1/blocks-531163-532333-txOut.csv'
    #txoutcsvpath = '/home/joadmin/bitcoin-tx/txOut/*.csv'  #local
    txout_df = spark.read.load(txoutcsvpath,format="csv", delimiter="\t", header=True)
    #print(type(txout_df), txout_df.printSchema())
    print("txin count:",txin_df.count())
    print("txout count:",txout_df.count())

    logger('get vertex...')
    vertex = getvertex(txin_df, txout_df)

#    wtriteVertexCsv(vertex)
#    vertex2 = readVertexCsv()  #??
#    print(vertex2.count())     #??


    logger('get edge...')
    edge = getedge(txin_df, txout_df)
    
    logger('create GraphFram...')
    from graphframes import *
    g = GraphFrame(vertex, edge)

    logger('get indegree...')
    indegrees_list = getIndegree(g)
    print(type(indegrees_list))

    logger('get outdegree...')
    outdegrees_list = getOutdegree(g)
    print(type(outdegrees_list))
    

    logger('get degree...')
    degrees_list = getDegree(g)
    print(type(degrees_list))
    

    logger('get pageRank...')
    pagerank_df1 = getPageRank(g)
    print(type(pagerank_df1))


    spark.stop()

    logger('Done!')