#!/usr/bin/env python
from imp import acquire_lock
VERSION = '2.1'

"""
based on: pgoapi - Pokemon Go API
Copyright (c) 2016 tjado <https://github.com/tejado>

Author: TC    <reddit.com/u/Tr4sHCr4fT>
Version: 1.5
"""

import os, json, sqlite3, argparse, logging
from time import sleep
from Queue import Queue
from threading import RLock

from fm.core import Work
from fm.db import check_db, fill_db
from fm.worker import Mastermind, DBworker, RPCworker, MapWorker 
from fm.utils import get_accounts, cover_circle, cover_square, PoGoAccount

log = logging.getLogger(__name__)

def init_config():
    parser = argparse.ArgumentParser()     
    logging.basicConfig(level=logging.INFO,\
                        format='[%(levelname)5s]%(module)10s %(asctime)s %(message)s', datefmt="%d.%m.%y %H:%M:%S")
    if os.path.isfile('DEBUG'): logging.getLogger(__name__).setLevel(logging.DEBUG)
    
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
    #parser.add_argument("--quelen", help="maximum cells in RAM", default=(1000), type=int)
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
        logging.getLogger(__name__).setLevel(logging.DEBUG)
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
    
    global qqtot 
    global killswitch
    killswitch = 0
    
    config = init_config()
    if not config:
        log.error('Configuration Error!'); return
        
    with sqlite3.connect(config.dbfile) as db:
        qqtot = db.cursor().execute("SELECT COUNT(*) FROM _queue").fetchone()[0]
    
    # some sanity checks   
    if qqtot == 0: log.info('Nothing to scan!'); return
    
    if not os.path.isfile(config.accfile):
        config.minions = 1
        config.accounts = [PoGoAccount('ptc',config.username,config.username)]
    else:
        config.accounts = get_accounts(config.accfile)
        if len(config.accounts) < config.minions: config.minions = len(config.accounts)

    if config.limit > 0 and config.limit < qqtot: qqtot = config.limit 

    if qqtot < config.minions: config.minions = qqtot

    # the fun begins
    Overseer = FastMapWorker(0, config)
    #Mastermind.setDaemon()
    Overseer.start()
    Overseer.join()

    log.info('Done!')



class FastMapWorker(Mastermind):
    def run(self):

        Minions = []
        accounts = self.config.accounts
        
        MapQ,    RPCq,    SQLq,    doneQ  \
      = Queue(), Queue(), Queue(), Queue()
        
        dblock = RLock()
        logf = open(self.config.logfile,'a')
        log.info('DB loaded...')
        
        log.debug('Filling Queue...')
        with sqlite3.connect(self.config.dbfile) as db:
            cells = [x[0] for x in db.cursor().execute("SELECT cell_id FROM '_queue' ORDER BY cell_id"
            " LIMIT %d" % qqtot).fetchall()]
        for cell in cells:
                RPCq.put(Work(cell,cell)) 

        n = (qqtot / self.config.minions)
        log.info('Total %d cells, %d Workers, %d cells each.' % (qqtot, self.config.minions, n))
        tt = (n * self.config.delay + 1); m, s = divmod(tt, 60); h, m = divmod(m, 60)
        log.info('ETA %d:%02d:%02d' % (h, m, s)); del n,h,m,s,tt
        
        log.debug('Initializing threads...')
        for minion in range(self.config.minions):
            Minions.append(RPCworker(minion+1, self.config, accounts[minion], RPCq, MapQ))
            sleep(3)

        MapT = MapWorker(0, self.config, MapQ, SQLq)
        DBt = DBworker(0, self.config, SQLq, doneQ, dblock)
 
        sleep(5)
        log.debug('Starting threads...')    
        for Minion in Minions:
            Minion.start()
            sleep(1)
        
        MapT.start()
        DBt.start()
        
        with sqlite3.connect(self.config.dbfile) as db:

            while not killswitch:
                
                while not doneQ.empty():
                    work = doneQ.get()
                    
                    if work.work is True:
                        try:
                            dblock.acquire()
                            db.cursor().execute("DELETE FROM _queue WHERE cell_id='%s'" % work.index)
                            db.commit() 
                            log.debug('Cell %s marked as done.' % work.index); self.pos += 1
                            log.info('Completed %d of %d Cells.' % (self.pos,qqtot))
                        except Exception as e:
                            doneQ.put(work)
                            log.error(e)
                            log.debug('Removing Cell %s from Queue failed...' % work.index)
                        
                        finally: dblock.release()
                    
                    elif work.work is False:
                        logf.write('%s\n' % work.index); self.pos += 1 
                        log.debug('Cell %s marked as failed.' % work.index)
                        
                    else: log.debug('Cell %s ignored.' % work.index)

                    sleep(1)
                    
                sleep(1); log.debug('Ping.')

        # cleaning up
        logf.flush()
        
        for Minion in Minions:
            Minion.join()
            
        MapT.join()
        DBt.join(3600)
        logf.close()

if __name__ == '__main__':
    main()
