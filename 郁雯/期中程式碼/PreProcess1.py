#處理抓取的交易資料，分成txIn和txOut.csv
# -*- coding: utf-8 -*-
import sys
import os
import pandas as pd
import numpy as np
import json
import csv

def main():
    #例:python PreProcess1.py D:\chainquery-python-master\tx-546020-546000\ tx-546020-546000
    if (len(sys.argv) != 3):
        print("ERROR: have to 2 arguments")
        print("例:python PreProcess1.py [資料夾root路徑] [資料夾名稱且為輸出檔名開頭]")
        print("例:python PreProcess1.py D:/chainquery-python-master/ tx-546020-546000")
        sys.exit()
    dirName = sys.argv[1]+ sys.argv[2]
    filePrefix = sys.argv[2]
    
    # Create nessesary folder
    try:
        os.mkdir(sys.argv[1] + filePrefix + "-In")
    except:
        pass

    try:
        os.mkdir(sys.argv[1] + filePrefix + "-Out")
    except:
        pass
    txInCSVFileName = sys.argv[1]+ filePrefix + "-In/" + filePrefix + "-txIn.csv"
    txOutCSVFileName = sys.argv[1]+ filePrefix + "-Out/" + filePrefix + "-txOut.csv"
    print(dirName)
    print(filePrefix)
    print(txInCSVFileName)
    print(txOutCSVFileName)
    
    filesPath = readFileList(dirName)
    processFile(filesPath, txInCSVFileName, txOutCSVFileName)
    
#寫入csv檔
def writeCSVFile(CSVfileName, df_data):
    #import csv
    #import os

    #寫入csv檔，檔案存在增加資料(不含欄位名)在後面，檔案不存在建立檔案並輸出含欄位名的資料
    # CSVfileName  = 'vin.csv' 
    if (os.path.exists(CSVfileName) == True):
        with open(CSVfileName, 'a', newline='') as csvfile:
            df_data.to_csv(csvfile, sep='\t', encoding='utf-8',header=None)    
    else:
        with open(CSVfileName, 'w', newline='') as csvfile:
            df_data.to_csv(csvfile, sep='\t', encoding='utf-8')    

#遞迴處理路徑下檔案與資料夾
def readFileList(dirName):
    #import os

    filesPath = []
    print(dirName)
    for root, dirs, files in os.walk(dirName):
        for f in files:
            filesPath.append(os.path.join(root, f))

    docCount = len(filesPath)  #檔案數量(交易數量)
    print(len(filesPath))
    # print(filesPath)
    return filesPath
    
def processFile(filesPath, txInCSVFileName, txOutCSVFileName):
    #import pandas as pd
    #import numpy as np
    #import json

    #filesPath = ["D:/chainquery-python-master/tx-546020-546000/000057b1a8805d8d9ca1313a545fdcd244627f463fd6917d47d5213212646ae8.json"]  #test
    for onefile in filesPath:
        with open(onefile, "r") as f:
            data = json.load(f)
            msg = processTxIn(data, txInCSVFileName)
            if msg == "TypeError":
                print("ERROR: %s TypeError" % f.name)
                continue
            processTxOut(data, txOutCSVFileName)
            
def processTxIn(data, txInCSVFileName):
    in_columns = ["txid", "vin_txid", "vin_vout_n", "address", "value", "blockhash", "blocktime"]
    in_df  = pd.DataFrame(columns=in_columns)  #建立空的DataFrame

    try:    
        txid = data['txid']
        blockhash = data['blockhash']
        blocktime = data['blocktime']

        for vinIndex in range(len(data['vin'])):
            if ('coinbase' in data['vin'][vinIndex]): #如果有coinbase代表是初始的幣
                vin_txid = np.nan
                vin_vout_n = np.nan
                vin_address = "No Inputs (Newly Generated Coins)"
                vin_value = np.nan
            else:
                vin_txid = data['vin'][vinIndex]['txid']
                vin_vout_n = data['vin'][vinIndex]['vout']
                vin_address = np.nan
                vin_value = np.nan

            #print(txid)
            #print(vin_txid)
            #print(vin_vout_n)
            #print(vin_address)
            #print(vin_value)
            #print(blockhash)
            #print(blocktime)
            in_record = [{"txid":txid, 
                       "vin_txid":vin_txid, 
                       "vin_vout_n":vin_vout_n,
                       "address":vin_address,
                       "value":vin_value,
                       "blockhash":blockhash,
                       "blocktime":blocktime}]

            in_df = in_df.append(in_record)
            in_df = in_df.reset_index(level=0, drop=True)  #重新設定index    
        
        writeCSVFile(txInCSVFileName, in_df)
        
    except TypeError:
        return "TypeError"
    except:
        print("ERROR: txid=%s, vin error." % txid)


def processTxOut(data, txOutCSVFileName):
    out_columns = ["txid", "vout_txid", "vout_n", "address", "value", "blockhash", "blocktime", "vout_type"]
    out_df = pd.DataFrame(columns=out_columns)  #建立空的DataFrame
    
    txid_hash = data['txid']
    vout_txid = data['txid']
    blockhash = data['blockhash']
    blocktime = data['blocktime']
    try:
        for voutindex in range(len(data['vout'])):
            vout_value = data['vout'][voutindex]['value']
            vout_n = data['vout'][voutindex]['n']
            vout_type = data['vout'][voutindex]['scriptPubKey']['type']
            if (vout_type == "pubkeyhash" or vout_type == "scripthash" or vout_type == "pubkey" or vout_type == "witness_v0_keyhash" or vout_type == "witness_v0_scripthash"):
                vout_address = data['vout'][voutindex]['scriptPubKey']['addresses'][0]
            elif (vout_type == "nulldata" or vout_type == "nonstandard"):
                vout_address = "Unparsed address"
            elif (vout_type == "multisig"):  #有多個帳號，只取第一個帳號代表
                vout_address = data['vout'][voutindex]['scriptPubKey']['addresses'][0]
            else:
                vout_address = np.nan
                print("WARN: txid:%s, vout_n:%d, type:%s, null address" % (txid_hash, vout_n, vout_type))

    #         print(txid_hash)
    #         print(vout_txid)
    #         print(vout_n)
    #         print(vout_address)
    #         print(vout_value)
    #         print(blockhash)
    #         print(blocktime)
    #         print(vout_type)
            out_record = [{"txid":txid_hash, 
                       "vout_txid":vout_txid, 
                       "vout_n":vout_n,
                       "address":vout_address,
                       "value":vout_value,
                       "blockhash":blockhash,
                       "blocktime":blocktime,
                       "vout_type":vout_type}]

            out_df = out_df.append(out_record)
            out_df = out_df.reset_index(level=0, drop=True)  #重新設定index
        
        writeCSVFile(txOutCSVFileName, out_df)
        
    except:
        print("ERROR: txid=%s, vout error." % txid_hash)

if __name__ == "__main__":
    main()