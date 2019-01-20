
# coding: utf-8

# In[132]:


import findspark
findspark.init('/home/joadmin/spark-2.1.2-bin-hadoop2.7/')
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName('gdelt_1').config("spark.jars.packages", "graphframes:graphframes:0.5.0-spark2.1-s_2.11").getOrCreate()

#讀入交易輸出部分的資料
csvpath = "/home/joadmin/tx-all-txOut-3.csv"
df_txOut = spark.read.load(csvpath,format="csv", delimiter="\t", header=True)

#讀入交易輸入部分的資料
csvpath = "/home/joadmin/tx-546020-546000-txIn.csv"
df_txIn = spark.read.load(csvpath,format="csv", delimiter="\t", header=True)


#顯示DataFrame欄位
print(df_txOut.describe)
print(df_txIn.describe)


# In[133]:


#檢視讀入資料
df_txIn.show()


# In[134]:


#用Spark SQL處理
df_txOut.createOrReplaceTempView("tableTXOUT")
df_txIn.createOrReplaceTempView("tableTXIN")

txInHaveAddress = spark.sql("SELECT tableTXIN.txid, tableTXIN.vin_txid, "+
                            "tableTXIN.vin_vout_n, COALESCE(tableTXIN.address, tableTXOUT.address) as address, "+
                            "COALESCE(tableTXIN.value, tableTXOUT.value) as value, "+
                            "tableTXIN.blockhash, tableTXIN.blocktime "+ 
                            "from tableTXIN left join tableTXOUT on tableTXIN.vin_txid = tableTXOUT.vout_txid "+
                            "and tableTXIN.vin_vout_n = tableTXOUT.vout_n")
txInHaveAddress.collect()

txInHaveAddress.show()



# In[20]:


#寫入csv檔
def writeCSVFile(CSVfileName, df_data):
    import csv
    import os

    with open(CSVfileName, 'w', newline='') as csvfile:
        df_data.to_csv(csvfile, sep='\t', encoding='utf-8')   
        


# In[135]:


#輸出
pd_df = txInHaveAddress.toPandas()
writeCSVFile('/home/joadmin/tx-546020-546000-txIn-onlyFindCurrentTxOut.csv',pd_df)

