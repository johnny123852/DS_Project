import os
import json
import chainquery as cq

START_HEIGHT = 546020
END_HEIGHT = 546000

# Create nessesary folder
try:
    os.mkdir('block')
except:
    pass

try:
    os.mkdir('tx')
except:
    pass


for height in range(START_HEIGHT, END_HEIGHT - 1, -1):
    block_hash = cq.getblockhash(height)
    print('block: %s' % block_hash)
    block = cq.getblock(block_hash)
    #print(type(json.dumps(block)))
    open('block/%s.json' % str(block_hash), 'w').write(json.dumps(block))
    txs = block['tx']
    for txid in txs:
        print('txid: %s' % txid)
        tx = cq.getrawtransaction(txid)
        open('tx/%s.json' % txid, 'w').write(json.dumps(tx))