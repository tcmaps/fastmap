#!/usr/bin/env python
VERSION = '2.1'

"""
based on: pgoapi - Pokemon Go API
Copyright (c) 2016 tjado <https://github.com/tejado>

Author: TC    <reddit.com/u/Tr4sHCr4fT>
Version: 1.5
"""

import os, time, json
import argparse, logging
import sqlite3

from Queue import Queue
from threading import Lock
from fm.core import Work
from fm.db import check_db, fill_db
from fm.worker import Overseer, DBworker, RPCworker, MapWorker
from fm.utils import get_accounts, cover_circle, cover_square, PoGoAccount

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
    parser.add_argument("--accounts", help="dont put anything here", default=None)
    parser.add_argument("--level", help="cell level used for tiling", default=12, type=int)
    parser.add_argument("--limit", help="maximum cells to scan", default=(0-1), type=int)
    parser.add_argument("-t", "--delay", help="rpc request interval", default=10, type=int)
    parser.add_argument("-m", "--minions", help="thread / worker count", default=10, type=int)
    parser.add_argument("-d", "--debug", help="Debug Mode", action='store_true', default=0)
    parser.add_argument("--logfile", help="failed scans log", default='failed.txt')
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
        from fm.utils import get_pos_by_name
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

    config = init_config()
    if not config:
        log.error('Configuration Error!'); return
        
    db = sqlite3.connect(config.dbfile)

    qqtot  = db.cursor().execute("SELECT COUNT(*) FROM _queue").fetchone()[0]
    
    # some sanity checks   
    if qqtot == 0: log.info('Nothing to scan!'); return
    
    if not os.path.isfile(config.accfile):
        config.minions = 1
        config.accounts = [PoGoAccount('ptc',config.username,config.username)]
    else:
        config.accounts = get_accounts(config.accfile)
        if len(config.accounts) < config.minions: config.minions = len(config.accounts)

    if config.limit > 0 and config.limit < qqtot: config.limit = qqtot

    if qqtot < config.minions: config.minions = qqtot
    
    qqpart = ( qqtot / config.minions )
    
    # acually load queue
    cells = [x[0] for x in db.cursor().execute("SELECT cell_id FROM '_queue'"
                                              " ORDER BY cell_id LIMIT %d" % qqtot).fetchall()]
    log.info('DB loaded.')
    
    queque = []
    # the fun begins
    for cell in cells: queque.append(cell)
    
    Mastermind = FastMapWorker(0, config, queque)
    Mastermind.setDaemon()
    Mastermind.start()
    Mastermind.join()

    log.info('Done!')



class FastMapWorker(Overseer):
    def run(self):
        
        Minions = []
        accounts = self.config.accounts
        MapQ, RPCq, SQLq, doneQ = Queue()
        dblock = Lock()
        db = sqlite3.connect(self.config.dbfile)
        logf = open(self.config.logfile,'a')
        
        if self.workload.__len__():
            
            for work in self.workload:
                RPCq.put(Work(work,work))

            for minion in range(self.config.minions):
                Minions.append(RPCworker(minion+1, self.config, accounts[minion], RPCq, MapQ))
                time.sleep(3)
            
            for Minion in Minions:
                Minion.start()
                time.sleep(1)
        
        self.wlen = len(self.workload)
        
        while len(self.workload) > 0:

            if not MapQ.empty():
                MapT = MapWorker(self.config, 0, MapQ, SQLq, self.config.dbfile, dblock); MapT.start()
            if not SQLq.empty():
                DBt = DBworker(self.config, 0, SQLq, doneQ); DBt.start()
            
            while not doneQ.empty():
                work = doneQ.get()
                self.pos += 1
                if work.work:
                    with dblock:
                        db.execute("DELETE FROM _queue WHERE cell_id='%s'" % work.index)
                else: logf.write(work.index + '\n')
                self.workload.remove(work.index)
                
            time.sleep(1)
            log.info('% of %' % (self.pos,self.wlen))


        # cleaning up
        logf.flush()
        
        for Minion in Minions:
            Minion.join()
            
        MapT.join()
        DBt.join(3600)
        logf.close()

if __name__ == '__main__':
    main()
