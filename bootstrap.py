#!/usr/bin/env python
VERSION = '2.1'

"""
based on: pgoapi - Pokemon Go API
Copyright (c) 2016 tjado <https://github.com/tejado>

Author: TC    <reddit.com/u/Tr4sHCr4fT>
Version: 1.5
"""

import os, sys, time, json
import argparse, logging
import sqlite3

from threading import Lock
from fastmap.db import check_db, fill_db
from fastmap.worker import FastMapWorker
from fastmap.utils import get_accounts, cover_circle, cover_square

log = logging.getLogger(__name__)

def init_config():
    parser = argparse.ArgumentParser()     
    logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(module)10s] [%(levelname)5s] %(message)s')

    load   = {}
    config_file = "config.json"
    if os.path.isfile(config_file):
        with open(config_file) as data:
            load.update(json.load(data))

    parser.add_argument("-a", "--auth_service", help="Auth Service ('ptc' or 'google')", default="ptc")
    parser.add_argument("-u", "--username", help="Username")
    parser.add_argument("-p", "--password", help="Password")
    parser.add_argument("-l", "--location", help="Location")
    parser.add_argument("-r", "--radius", help="area circle radius", type=int)
    parser.add_argument("-w", "--width", help="area square width", type=int)
    parser.add_argument("--dbfile", help="DB filename", default='db.sqlite')
    parser.add_argument("--accfile", help="ptc account list", default='accounts.txt')
    parser.add_argument("--level", help="cell level used for tiling", default=12, type=int)
    parser.add_argument("-t", "--delay", help="rpc request interval", default=10, type=int)
    parser.add_argument("-m", "--minions", help="thread / worker count", default=10, type=int)
    parser.add_argument("-d", "--debug", help="Debug Mode", action='store_true', default=0)    
    config = parser.parse_args()

    for key in config.__dict__:
        if key in load and config.__dict__[key] == None:
            config.__dict__[key] = load[key]

    if config.auth_service not in ['ptc', 'google']:
        log.error("Invalid Auth service specified! ('ptc' or 'google')")
        return None

    if config.debug:
        logging.getLogger("requests").setLevel(logging.DEBUG)
        logging.getLogger("pgoapi").setLevel(logging.DEBUG)
        logging.getLogger("rpc_api").setLevel(logging.DEBUG)
    else:
        logging.getLogger("requests").setLevel(logging.WARNING)
        logging.getLogger("pgoapi").setLevel(logging.WARNING)
        logging.getLogger("rpc_api").setLevel(logging.WARNING)
   
    dbversion = check_db(config.dbfile)     
    if dbversion != VERSION:
        log.error('Database version mismatch! Expected {}, got {}...'.format(VERSION,dbversion))
        return
    
    if config.location:
        from fastmap.utils import get_pos_by_name
        lat, lng, alt = get_pos_by_name(config.location); del alt
        if config.radius:
            cells = cover_circle(lat, lng, config.radius, config.level)
        elif config.width:
            cells = cover_square(lat, lng, config.width, config.level)
        else: log.error('Area size not given!'); return
        log.info('Added %d cells to scan queue.' % fill_db(config.dbfile, cells))
        del cells, lat, lng
    
    if config.minions < 1: config.minions = 1
    
    return config

def main():
    dblock = Lock()
    
    config = init_config()
    if not config:
        log.error('Configuration Error!'); return
        
    db = sqlite3.connect(config.dbfile)

    ques  = db.cursor().execute("SELECT COUNT(*) FROM _queue").fetchone()[0]
    quepw = (ques/config.minions)
    
    # some sanity checks   
    if ques == 0: log.info('Nothing to scan!'); return
    
    if ques < config.minions: config.minions = ques
    
    if not os.path.isfile(config.accfile): config.minions = 1
    else: accs = get_accounts(config.accfile)

    if len(accs) < config.minions: config.minions = len(accs)
    
    
    # the fun begins
    if config.minions == 1:
        
        dummy = db.cursor().execute("SELECT cell_id FROM '_queue' WHERE cell_level = %d ORDER BY cell_id "\
        % (config.level)).fetchall()
        # http://stackoverflow.com/questions/3614277/how-to-strip-from-python-pyodbc-sql-returns 
        queue = [x[0] for x in dummy]
        
        T = FastMapWorker(0, config, config, queue, dblock)
        T.start()
    
    # fairly distributing work    
    else:
        tqueue = [] 
        for minion in range(0,config.minions):    
            
            for m in range(0,config.minions):
                dummy = db.cursor().execute("SELECT cell_id FROM '_queue' WHERE cell_level = %d ORDER BY cell_id "\
                                            "LIMIT %d,%d" % (config.level,(m * quepw), quepw)).fetchall()
                tqueue.append([x[0] for x in dummy])
            
            log.info('(%2d) Starting Thread %2d...' % (minion+1,minion+1))
                
            Minion = FastMapWorker(minion+1, config, accs[minion], tqueue[minion], dblock)
            Minion.start()
            time.sleep(1)
        
        
        Minion.join()
        # one must always do the leftover
        if config.minions * quepw < ques:
            log.info("Rest: {}".format(ques - config.minions * quepw))
            log.info('Please run again with -m 1 to complete!')
            
        

if __name__ == '__main__':
    main()
