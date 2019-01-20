#找txIn的address，從txOut找，沒有找到再從或網路抓取

import sys
import os
import numpy as np
import pandas as pd
import chainquery as cq
import json

#-----------------------------------------------------------------
#處理交易的Out資料
def processTxOut(data, txOutCSVFileName, n):
    #out_columns = ["txid", "vout_txid", "vout_n", "address", "value", "blockhash", "blocktime", "vout_type"]
    #out_df = pd.DataFrame(columns=out_columns)  #建立空的DataFrame

    try:    
        txid_hash = data['txid']
        vout_txid = data['txid']
        blockhash = data['blockhash']
        blocktime = data['blocktime']

        for voutindex in range(len(data['vout'])):
            vout_n = data['vout'][voutindex]['n']
            if (vout_n == n):
                vout_value = data['vout'][voutindex]['value']
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

                return vout_address, vout_value

        #         print(txid_hash)
        #         print(vout_txid)
        #         print(vout_n)
        #         print(vout_address)
        #         print(vout_value)
        #         print(blockhash)
        #         print(blocktime)
        #         print(vout_type)
    #             out_record = [{"txid":txid_hash, 
    #                        "vout_txid":vout_txid, 
    #                        "vout_n":vout_n,
    #                        "address":vout_address,
    #                        "value":vout_value,
    #                        "blockhash":blockhash,
    #                        "blocktime":blocktime,
    #                        "vout_type":vout_type}]
            


#             out_df = out_df.append(out_record)
#             out_df = out_df.reset_index(level=0, drop=True)  #重新設定index
        
#         writeCSVFile(txOutCSVFileName, out_df)
        
    except TypeError:
        print("ERROR:TypeError")
        return np.nan, np.nan
    
    except:
        print("ERROR: txid=%s, vout error." % txid_hash)
        return np.nan, np.nan

#-----------------------------------------------------------------        
#寫入csv檔
def writeCSVFile(CSVfileName, df_data):
    import csv
    import os

    #寫入csv檔，檔案存在增加資料(不含欄位名)在後面，檔案不存在建立檔案並輸出含欄位名的資料
    # CSVfileName  = 'vin.csv' 
#     if (os.path.exists(CSVfileName) == True):
#         with open(CSVfileName, 'a', newline='') as csvfile:
#             df_data.to_csv(csvfile, sep='\t', encoding='utf-8',header=None)    
#     else:

    with open(CSVfileName, 'w', newline='') as csvfile:
        df_data.to_csv(csvfile, sep='\t', encoding='utf-8')    
#-----------------------------------------------------------------

def main():
    #例:python PreProcess2.py tx-Out tx-txIn
    if (len(sys.argv) != 5):
        print("ERROR: have to 4 arguments")
        print("程式要與資料夾同一個目錄下")
        print("例:python PreProcess2.py [In資料夾名稱和csv檔案] [In資料夾名稱和csv檔案] [In資料夾名稱和輸出csv檔案] [儲存其他InTx資料夾]")
        print("例:python PreProcess2.py tx-In/tx-txIn.csv tx-Out/tx-txOut.csv  tx-In/tx-txIn-2.csv tx-In/OtherInTx/")
        sys.exit()
    filesPath_In = []
    filesPath_In.append(sys.argv[1])
    filesPath_Out = []
    filesPath_Out.append(sys.argv[2])
    txInCSVFileName_AfterProcess = sys.argv[3]
    txInOtherTx = sys.argv[4]
    
    try:
        os.mkdir(txInOtherTx)
    except:
        pass

    
    
    print(filesPath_In)
    print(filesPath_Out)
    print(txInCSVFileName_AfterProcess)
    print(txInOtherTx)
    
    ##[read file]
    # Import the csv data: in_df
    #filesPath_In = ["tx-In/tx-txIn.csv"]  #test
    for onefile in filesPath_In:
        in_df = pd.read_csv(onefile, delimiter= '\t')

    #print(in_df.shape)

    in_df = in_df.drop(columns = 'Unnamed: 0',axis= 1)  #刪掉第一欄index,因為匯入會有新的
    in_df = in_df.reset_index(level=0, drop=True)  #重新設定index  
    #print(in_df.head(1))


    # Import the csv data: out_df
    #filesPath_Out = ["tx-Out/tx-txOut.csv"]  #test
    for onefile in filesPath_Out:
        out_df = pd.read_csv(onefile, delimiter= '\t')

    #print(out_df.shape)
    out_df = out_df.drop(columns = 'Unnamed: 0',axis= 1)  #刪掉第一欄index,因為匯入會有新的
    out_df = out_df.reset_index(level=0, drop=True)  #重新設定index  
    #print(out_df.head(1))

    #---------------------------
    ##[process]
    #import chainquery as cq
    #import json

    inNaIndexList = in_df[in_df['address'].isna()].index
    for inIndex in inNaIndexList:
        if inIndex % 5000 == 0:
            print(inIndex)
        oneVin_txid = in_df.loc[inIndex,'vin_txid']
        oneVin_vout_n = in_df.loc[inIndex,'vin_vout_n']
        #print("oneVin_txid:%s, oneVin_vout_n:%s" % (oneVin_txid, oneVin_vout_n))
    #     recordIndex = out_df[(out_df.vout_txid == 'bcdf802cef9fceeeb88cbe91b6ccfeca0e5b8dddf529d4b9ffc7e5d7d5123e10') & (out_df.vout_n == 3)].index[0]
        outGet_df =  out_df[(out_df.vout_txid == oneVin_txid) & (out_df.vout_n == oneVin_vout_n)]
        if outGet_df.empty:  #代表out_df沒有找到相同的txid和n資料，需要上網抓取
            #print("not find.")
            tx = cq.getrawtransaction(oneVin_txid)
            data = json.loads(json.dumps(tx))
            open('%s%s.json' % (txInOtherTx, oneVin_txid), 'w').write(json.dumps(tx))
            address, value_find = processTxOut(data,"",oneVin_vout_n)
            in_df.loc[inIndex,'address'] = address
            in_df.loc[inIndex,'value'] = value_find
            #print(in_df.loc[inIndex,'address'], in_df.loc[inIndex,'value'])
        else:  #代表out_df有找到相同的txid和n資料，直接取address更新
            recordIndex = outGet_df.index[0]
            in_df.loc[inIndex,'address'] = out_df.loc[recordIndex,'address']
            in_df.loc[inIndex,'value'] = out_df.loc[recordIndex,'value']
            #print(in_df.loc[inIndex,'address'], in_df.loc[inIndex,'value'])
            
    #txInCSVFileName_AfterProcess = "tx-In/tx-txIn-2.csv"  #test
    writeCSVFile(txInCSVFileName_AfterProcess, in_df)

    
if __name__ == "__main__":
    main()
