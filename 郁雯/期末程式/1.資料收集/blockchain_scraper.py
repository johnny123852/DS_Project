#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import requests
import json
import os
import logging
import time



def get_block_url(block_height, format='json'):
    return f'https://blockchain.info/block-height/{block_height}?format={format}'

def get_block_data(block_height, format='json'):
    try:
        block_url = get_block_url(block_height, format)

        for i in range(0, 5):
            try:
                response = requests.get(url=block_url)
                block = response.json()
                break
            except json.JSONDecodeError as e:
                block = None
                if response.text[0] == 'B': # Block not found
                    return None, False

        return block, True

    except requests.exceptions.RequestException as e:
        logging.exception(e)


def dump_block(block):
    if not os.path.exists('blocks'):
        os.mkdir('blocks')
    with open(f"./blocks/{block['blocks'][0]['height']}.json", 'w') as output:
        json.dump(block, output)

def exists_block(height):
    return os.path.isfile(f"./blocks/{height}.json")


def download(height=0):
    if len(sys.argv) == 3:
        height = int(sys.argv[1])
        stopheight = int(sys.argv[2])
        print("height:", height, "stopheight:", stopheight)

    running = True
    while(running):
        if (height > stopheight):
            running = False
            continue
            
        if exists_block(height):
            logging.info(f'Skipped block {height}')
            height += 1
            continue

        logging.info(f'Retrieving block #{height}')
        block, found = get_block_data(height)

        if found:
            logging.info(f'Retrieved block #{height}\n * * *')
        else:
            logging.info(f'[WARN]not found block #{height}\n * * *')

        dump_block(block)
        height += 1


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    download()
