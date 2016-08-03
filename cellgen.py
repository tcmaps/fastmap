#!/usr/bin/env python

import os
import sys
import json
import time
import struct
import logging
import argparse
import sqlite3

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(module)10s] [%(levelname)5s] %(message)s')
log = logging.getLogger(__name__)

import utils

def init_config():
    parser = argparse.ArgumentParser()
    config_file = "config.json"

    # If config file exists, load variables from json
    load   = {}
    if os.path.isfile(config_file):
        with open(config_file) as data:
            load.update(json.load(data))

    # Read passed in Arguments
    required = lambda x: not x in load
    parser.add_argument("-l", "--location", help="Location", required=required("location"))
    parser.add_argument("-r", "--cells", help="Amount of cells to walk up and down the Hilbert curve", default=1000)
    parser.add_argument("-d", "--debug", help="Debug Mode", action='store_true')
    parser.set_defaults(DEBUG=False, TEST=False)
    config = parser.parse_args()

    # Passed in arguments shoud trump
    for key in config.__dict__:
        if key in load and config.__dict__[key] == None:
            config.__dict__[key] = load[key]

    return config

def main():
    
    config = init_config()
    if not config:
        return

    position = utils.get_pos_by_name(config.location)
    if not position:
        return
    log.info('Your given location: %s', config.location)
    log.info('lat/long/alt: %s %s %s', *position)
    
    db = sqlite3.connect('db.sqlite')
    
    cell_ids = utils.get_cell_walk(position[0], position[1], int(config.cells)) 
    
    for cell in cell_ids:
        db.cursor().execute("REPLACE INTO cells (cell_id) VALUES ({})".format(cell))
    db.commit()
    log.info('%d cells inserted into DB',len(cell_ids))

if __name__ == '__main__':
    main()