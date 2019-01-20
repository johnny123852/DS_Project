import json
#import urllib2
import urllib
from bs4 import BeautifulSoup

CHAIN_QUERY_URL = 'http://chainquery.com/bitcoin-api'

def chain_query(function, args = []):
    print(function, args)
    url = '%s/%s/%s' % (CHAIN_QUERY_URL, function, '/'.join(args))
    if len(args) == 0:
        url = url.strip('/')

    try:
        #html = urllib2.urlopen(url).read()
        html = urllib.request.urlopen(url).read()
        res = parse_html(html)
        return res
    except Exception as ex:
        print(ex)
        return False

def getbestblockhash():
    result = chain_query('getbestblockhash')
    return result

def getblock(block_hash, output = 'true'):
    choices = ['true', 'false']
    if not output in choices:
        return 'choice: %s, not %s' % (str(choices), str(output))
    result = chain_query('getblock', [block_hash, output])
    return result

def getblockchaininfo():
    result = chain_query('getblockchaininfo')
    return result

def getblockcount():
    result = chain_query('getblockcount')
    return result

def getblockhash(block_height):
    result = chain_query('getblockhash', [str(block_height)])
    return result

def getdifficulty():
    result = chain_query('getdifficulty')
    return result

def getrawtransaction(txid, decode = '1'):
    choices = ['0', '1']
    if not decode in choices:
        return 'choice: %s, not %s' % (str(choices), str(output))
    result = chain_query('getrawtransaction', [txid, decode])
    return result

def parse_html(html):
    soup = BeautifulSoup(html, 'html.parser')
    pre = str(soup.find('pre'))
    res = json.loads(pre[5:-7])
    if res['error'] != None:
        return res['error']['message']
    else:
        return res['result']