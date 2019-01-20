##找出資料為False，代表上次抓取失敗，重新抓資料
##相同指令可重複執行
# -*- coding: utf-8 -*-
import sys
import os
import pandas as pd
import numpy as np
import json
import csv
import chainquery as cq

def main():
    #例:python PreProcess1.py D:\chainquery-python-master\tx-546020-546000\ tx-546020-546000
    if (len(sys.argv) != 3):
        print("ERROR: have to 2 arguments")
        print("例:python PreProcess1.py [資料夾root路徑] [資料夾名稱且為輸出檔名開頭]")
        print("例:python PreProcess1.py D:/chainquery-python-master/ tx-546020-546000")
        sys.exit()
    dirName = sys.argv[1]+ sys.argv[2]
    
    filesPath = readFileList(dirName)
    processFile(filesPath, dirName)
    

#遞迴處理路徑下檔案與資料夾
def readFileList(dirName):
    #import os

    filesPath = []
    print(dirName)
    for root, dirs, files in os.walk(dirName):
        for f in files:
            filesPath.append(os.path.join(root, f))

    docCount = len(filesPath)  #檔案數量(交易數量)
    print("Total %i files." % len(filesPath))
    return filesPath
    
def processFile(filesPath, dirName):
    #import pandas as pd
    #import numpy as np
    #import json

    #filesPath = ["D:/chainquery-python-master/tx-546020-546000/000057b1a8805d8d9ca1313a545fdcd244627f463fd6917d47d5213212646ae8.json"]  #test
    for onefile in filesPath:
        with open(onefile, "r") as f:
            data = json.load(f)
            txid = os.path.basename(f.name).split('.')[0] #由檔名取得交易id
            if (data == False ):  #資料為False時，代表上次抓取失敗
                tx = cq.getrawtransaction(txid)
                open(dirName+'/%s.json' % txid, 'w').write(json.dumps(tx))
            
            

if __name__ == "__main__":
    main()