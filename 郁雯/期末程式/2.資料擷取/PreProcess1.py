#處理抓取的交易資料，分成txIn和txOut.csv
# -*- coding: utf-8 -*-
import sys
import os
import pandas as pd
import numpy as np
import json
import csv
from decimal import *

def main():
    #例:python PreProcess1.py D:\抓比特幣交易資料\ blocks-1-10
    if (len(sys.argv) != 3):
        print("ERROR: have to 2 arguments")
        print("例:python PreProcess1.py [資料夾root路徑] [資料夾名稱且為輸出檔名開頭]")
        print("例:python PreProcess1.py D:/抓比特幣交易資料/ blocks-1-10")
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
    
    try:
        os.mkdir(sys.argv[1] + filePrefix + "-Fee")
    except:
        pass
    
    txInCSVFileName = sys.argv[1]+ filePrefix + "-In/" + filePrefix + "-txIn.csv"
    txOutCSVFileName = sys.argv[1]+ filePrefix + "-Out/" + filePrefix + "-txOut.csv"
    txFeeCSVFileName =  sys.argv[1]+ filePrefix + "-Fee/" + filePrefix + "-txFee.csv"
    print(dirName)
    print(filePrefix)
    print(txInCSVFileName)
    print(txOutCSVFileName)
    print(txFeeCSVFileName)
    
    filesPath = readFileList(dirName)
    processFile(filesPath, txInCSVFileName, txOutCSVFileName, txFeeCSVFileName)
    
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
    
def processFile(filesPath, txInCSVFileName, txOutCSVFileName, txFeeCSVFileName):
    #import pandas as pd
    #import numpy as np
    #import json

    #filesPath = ["D:/chainquery-python-master/tx-546020-546000/000057b1a8805d8d9ca1313a545fdcd244627f463fd6917d47d5213212646ae8.json"]  #test
    for onefile in filesPath:
        with open(onefile, "r") as f:
            print("f.name:",f.name)
            data = json.load(f)
            tx_in_sum = 0
            tx_out_sum = 0
            processTxIn(data, txInCSVFileName, txOutCSVFileName, txFeeCSVFileName)
            #tx_in_sum = processTxIn(data, txInCSVFileName)
            #if msg == "TypeError":
            #    print("ERROR: %s TypeError" % f.name)
            #    continue
            #tx_out_sum = processTxOut(data, txOutCSVFileName)
            #print("tx_in_sum:",tx_in_sum, "tx_out_sum:" ,tx_out_sum)
            
def processTxIn(data, txInCSVFileName, txOutCSVFileName, txFeeCSVFileName):
    in_columns = ["txid", "vin_txid", "vin_vout_n", "address", "value", "blockhash", "blocktime", "vin_script", "tx_index"]
    in_df  = pd.DataFrame(columns=in_columns)  #建立空的DataFrame
    out_columns = ["txid", "vout_txid", "vout_n", "address", "value", "blockhash", "blocktime", "vout_type", "vout_script", "tx_index", "block_reward"]
    out_df = pd.DataFrame(columns=out_columns)  #建立空的DataFrame
    fee_columns = ["txid", "blocktime", "in_number", "out_number", "fee", "fee_per_byte", "blockhash", "preblockhash"]
    fee_df = pd.DataFrame(columns=fee_columns)  #建立空的DataFrame
    
    #try:    
    for txIndex in range(len(data['blocks'][0]['tx'])):
        getcontext().prec = 9  #小數第9位進位
        
        tx_in_sum = 0
        tx_out_sum = 0
        
        txid = ''
        blockhash = ''
        blocktime = ''
        tx_index = ''
        
        vin_txid = ''
        vin_vout_n = ''
        vin_address = ''
        vin_value = 0
        vin_script = ''
        
        vout_txid = ''
        vout_address = ''
        vout_value = 0
        vout_n = ''
        vout_type = ''
        vout_script = ''
        block_reward = 'N'
        
        in_number = 0
        out_number = 0
        fee = 0
        fee_per_byte = 0
        preblockhash = ''
        
        
        txid = data['blocks'][0]['tx'][txIndex]['hash']
        blockhash = data['blocks'][0]['hash']
        blocktime = data['blocks'][0]['time']
        tx_index  = data['blocks'][0]['tx'][txIndex]['tx_index']
        
        
        #print(txid)
        #print(blockhash)
        #print(blocktime)
        #print(tx_index)
        
        #in
        for vinIndex in range(len(data['blocks'][0]['tx'][txIndex]['inputs'])):
            #print("txIndex:",txIndex,"txid:",txid,"vinIndex:",vinIndex)
            if ('prev_out' not in data['blocks'][0]['tx'][txIndex]['inputs'][vinIndex]): #如果沒有prev_out代表是初始的幣
                vin_txid = np.nan
                vin_vout_n = np.nan
                vin_address = "No Inputs (Newly Generated Coins)"
                vin_value = 0    
                vin_script = np.nan
            else:
                vin_txid = data['blocks'][0]['tx'][txIndex]['inputs'][vinIndex]['prev_out']['tx_index']
                vin_vout_n = data['blocks'][0]['tx'][txIndex]['inputs'][vinIndex]['prev_out']['n']
                if ('addr' not in data['blocks'][0]['tx'][txIndex]['inputs'][vinIndex]['prev_out']): #沒有addr,vin_address為"Unparsed address"
                    vin_address = "Unparsed address"
                else:
                    vin_address = data['blocks'][0]['tx'][txIndex]['inputs'][vinIndex]['prev_out']['addr']
                vin_value = Decimal(data['blocks'][0]['tx'][txIndex]['inputs'][vinIndex]['prev_out']['value']) / Decimal(100000000)
                vin_script = data['blocks'][0]['tx'][txIndex]['inputs'][vinIndex]['prev_out']['script']

            if txIndex != 0:
                tx_in_sum = Decimal(tx_in_sum) + Decimal(vin_value)
            #print("len:",len(data['blocks'][0]['tx']))
            #print("txIndex:",txIndex)
            #print(txid)
            #print(vin_txid)
            #print(vin_vout_n)
            #print(vin_address)
            #print(vin_value)
            #print(blockhash)
            #print(blocktime)
            #print(vin_script)
            #print(tx_index)
            in_record = [{"txid":txid, 
                       "vin_txid":vin_txid, 
                       "vin_vout_n":vin_vout_n,
                       "address":vin_address,
                       "value":vin_value,
                       "blockhash":blockhash,
                       "blocktime":blocktime,
                       "vin_script":vin_script,
                       "tx_index":tx_index }]

            in_df = in_df.append(in_record)
            in_df = in_df.reset_index(level=0, drop=True)  #重新設定index    
        
        #out
        for voutindex in range(len(data['blocks'][0]['tx'][txIndex]['out'])):
            vout_txid = data['blocks'][0]['tx'][txIndex]['out'][voutindex]['tx_index']
            if ('addr' in data['blocks'][0]['tx'][txIndex]['out'][voutindex]): #如果有addr代表有addr
                vout_address = data['blocks'][0]['tx'][txIndex]['out'][voutindex]['addr']
            else:
                vout_address = np.nan
            vout_value = Decimal(data['blocks'][0]['tx'][txIndex]['out'][voutindex]['value']) / Decimal(100000000)
            vout_n = data['blocks'][0]['tx'][txIndex]['out'][voutindex]['n']
            vout_type = data['blocks'][0]['tx'][txIndex]['out'][voutindex]['type']
            vout_script = data['blocks'][0]['tx'][txIndex]['out'][voutindex]['script']
            
            if txIndex != 0:
                tx_out_sum = Decimal(tx_out_sum) + Decimal(vout_value)
                block_reward = 'N'
            else:
                block_reward = 'Y'
            #print("len:",len(data['blocks'][0]['tx']))
            #print(txid_hash)
            #print(vout_txid)
            #print(vout_n)
            #print(vout_address)
            #print(vout_value)
            #print(blockhash)
            #print(blocktime)
            #print(vout_type)
            #print(vout_script)
            #print(tx_index)
            out_record = [{"txid":txid, 
                       "vout_txid":vout_txid, 
                       "vout_n":vout_n,
                       "address":vout_address,
                       "value":vout_value,
                       "blockhash":blockhash,
                       "blocktime":blocktime,
                       "vout_type":vout_type,
                       "vout_script":vout_script,
                       "tx_index":tx_index,
                       "block_reward":block_reward}]

            out_df = out_df.append(out_record)
            out_df = out_df.reset_index(level=0, drop=True)  #重新設定index
        
        
        in_number = data['blocks'][0]['tx'][txIndex]['vin_sz']
        out_number = data['blocks'][0]['tx'][txIndex]['vout_sz']
        fee = Decimal(tx_in_sum) - Decimal(tx_out_sum)
        fee_per_byte = Decimal(fee)/Decimal(in_number * 180 + out_number * 34 +10)
        preblockhash = data['blocks'][0]['prev_block']
        fee_record = [{"txid":txid, 
                   "blocktime":blocktime,
                   "in_number":in_number, 
                   "out_number":out_number,
                   "fee":fee,
                   "fee_per_byte":fee_per_byte,
                   "blockhash":blockhash,
                   "preblockhash":preblockhash}]
                   
        fee_df = fee_df.append(fee_record)
        fee_df = fee_df.reset_index(level=0, drop=True)  #重新設定index
        
    
    writeCSVFile(txInCSVFileName, in_df)
    writeCSVFile(txOutCSVFileName, out_df)
    writeCSVFile(txFeeCSVFileName, fee_df)
    #return tx_in_sum
    #except TypeError:
    #    print("ERROR: TypeError txid=%s, vin error." % txid)
    #except:
    #    print("ERROR: txid=%s, vin error." % txid)


def processTxOut(data, txOutCSVFileName):
    out_columns = ["txid", "vout_txid", "vout_n", "address", "value", "blockhash", "blocktime", "vout_type", "vout_script", "tx_index"]
    out_df = pd.DataFrame(columns=out_columns)  #建立空的DataFrame
    tx_out_sum = 0

    try:
    
        for txIndex in range(len(data['blocks'][0]['tx'])):
            
            txid_hash = data['blocks'][0]['tx'][txIndex]['hash']
            blockhash = data['blocks'][0]['hash']
            blocktime = data['blocks'][0]['time']
            tx_index  = data['blocks'][0]['tx'][txIndex]['tx_index']
            
            for voutindex in range(len(data['blocks'][0]['tx'][txIndex]['out'])):
                vout_txid = data['blocks'][0]['tx'][txIndex]['out'][voutindex]['tx_index']
                if ('addr' in data['blocks'][0]['tx'][txIndex]['out'][voutindex]): #如果有addr代表有addr
                    vout_address = data['blocks'][0]['tx'][txIndex]['out'][voutindex]['addr']
                else:
                    vout_address = np.nan
                vout_value =data['blocks'][0]['tx'][txIndex]['out'][voutindex]['value'] / 100000000
                vout_n = data['blocks'][0]['tx'][txIndex]['out'][voutindex]['n']
                vout_type = data['blocks'][0]['tx'][txIndex]['out'][voutindex]['type']
                vout_script = data['blocks'][0]['tx'][txIndex]['out'][voutindex]['script']
                
                if txIndex != 0:
                    tx_out_sum = tx_out_sum + vout_value
                #print("len:",len(data['blocks'][0]['tx']))
                #print(txid_hash)
                #print(vout_txid)
                #print(vout_n)
                #print(vout_address)
                #print(vout_value)
                #print(blockhash)
                #print(blocktime)
                #print(vout_type)
                #print(vout_script)
                #print(tx_index)
                out_record = [{"txid":txid_hash, 
                           "vout_txid":vout_txid, 
                           "vout_n":vout_n,
                           "address":vout_address,
                           "value":vout_value,
                           "blockhash":blockhash,
                           "blocktime":blocktime,
                           "vout_type":vout_type,
                           "vout_script":vout_script,
                           "tx_index":tx_index}]

                out_df = out_df.append(out_record)
                out_df = out_df.reset_index(level=0, drop=True)  #重新設定index
        
        writeCSVFile(txOutCSVFileName, out_df)
        return tx_out_sum
    except:
        print("ERROR: txid=%s, vout error." % txid_hash)

        

if __name__ == "__main__":
    main()