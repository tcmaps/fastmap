#!/usr/bin/env python

"""
based on: pgoapi - Pokemon Go API
Copyright (c) 2016 tjado <https://github.com/tejado>

Author: TC    <reddit.com/u/Tr4sHCr4fT>
Version: 2.0
"""

import os, json, sqlite3, argparse, logging
from time import sleep
from Queue import Queue
from threading import RLock

from fastmap.core import Work, Mastermind, PoisonPill
from fastmap.db import check_db, fill_db
from fastmap.worker import MapRequest, MapParse, DataBase 
from fastmap.utils import get_accounts, cover_circle, cover_square, PoGoAccount

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
    parser.add_argument("--quelen", help="maximum cells in RAM", default=(100), type=int)
    parser.add_argument("--trsh", help="error threshold", default=(10), type=int)
    parser.add_argument("-t", "--delay", help="rpc request interval", default=10, type=int)
    parser.add_argument("-m", "--minions", help="thread / worker count", default=10, type=int)
    parser.add_argument("-d", "--debug", help="Debug Mode", action='store_true', default=0)
    parser.add_argument("--logfile", help="failed scans log", default='bootstrap.log')
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
   
    if not check_db(config.dbfile):     
        return None
    
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
    
    global qqtot 
    global killswitch
    killswitch = False
    
    config = init_config()
    if not config:
        log.error('Configuration Error!'); return
        
    with sqlite3.connect(config.dbfile) as db:
        qqtot = db.cursor().execute("SELECT COUNT(*) FROM _queue WHERE scan_status=0").fetchone()[0]
    
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
    
    n = (qqtot / config.minions)
    log.info('Total %d cells, %d Workers, %d cells each.' % (qqtot, config.minions, n))
    ttot = (n * config.delay + 1); m, s = divmod(ttot, 60); h, m = divmod(m, 60)
    log.info('ETA %d:%02d:%02d' % (h, m, s)); del n,h,m,s
    
    # last chance to break
    for i in xrange(3):
        log.info('Start in %d...' % (3-i)); sleep(1)
    
    # the fun begins
    Overseer = BootStrap(0, config)
    BootStrap.setDaemon(Overseer, daemonic=True)
    Overseer.start()
    
    try: Overseer.join(ttot+100)
    except KeyboardInterrupt: killswitch = None
    finally: Overseer.join(60)

    log.info('Dekimashita!')



class BootStrap(Mastermind):
    
    def run(self):
        
        killswitch, dontkillme = False, True
        
        Minions = []
        accounts = self.config.accounts
        
        MapQ,    RPCq,    SQLq,    doneQ  \
      = Queue(), Queue(), Queue(), Queue()
        
        dblock = RLock()

        logf = logging.getLogger('file'); logf.addHandler(logging.FileHandler(self.config.logfile)\
                                        .setFormatter(logging.Formatter('%(asctime)s - %(message)s')))
        log.info('Loading DB...'); logging.getLogger('file').setLevel(logging.ERROR)
        
        with sqlite3.connect(self.config.dbfile) as db:
            cells = [x[0] for x in db.cursor().execute("SELECT cell_id FROM '_queue' WHERE scan_status=0 "
                    "ORDER BY cell_id LIMIT %d" % (self.config.quelen*self.config.minions)).fetchall()]
        qqtfill = len(cells)
        log.debug('Filling Queue... (%d cells)' % (qqtfill))
        if len(cells) > 0:
            for cell in cells:
                RPCq.put(Work(cell,cell))
        
        log.info('Initializing threads...')
        for m in xrange(self.config.minions):
            Minions.append(MapRequest(m+1, self.config, RPCq, MapQ, accounts[m]))

        MapT = MapParse(0, self.config, MapQ, SQLq)
        DBt = DataBase(0, self.config, SQLq, doneQ, locks=[dblock])

        log.info('Starting threads...')    
        for Minion in Minions:
            Minion.start()
            sleep(5)
        
        MapT.start()
        DBt.start()
        
        self.ok, self.fail = 0,0

        while dontkillme:
            try: 
                while not doneQ.empty():
                    work = doneQ.get()
                    
                    if type(work) is PoisonPill:
                        if killswitch is False: killswitch = None
                        continue
                    
                    if work.work is True:
    
                        log.debug('Cell %s marked as done.' % work.index); self.ok += 1
                        log.info('Completed %d of %d Cells.' % (self.pos,qqtot))
    
                    elif work.work is False:
                        logf.error('%s\n' % work.index); self.fail += 1 
                        log.debug('Cell %s marked as failed.' % work.index)
                        
                    else: log.debug('Cell %s ignored.' % work.index); self.pos += 1
                
                self.pos = (self.ok + self.fail)
                log.debug('%d Cells scanned... %d ok, %d failed, %d lost' % (self.pos, self.ok, self.fail,\
                                                                                self.pos - self.ok - self.fail))
                # Hold work only partially in RAM
                if not killswitch and RPCq.empty() and qqtfill < qqtot:
                    
                    MapQ.join(); SQLq.join()
                    
                    cells = [x[0] for x in db.cursor().execute("SELECT cell_id FROM '_queue' WHERE scan_status=0 "
                    "ORDER BY cell_id LIMIT %d" % (self.config.quelen*self.config.minions)).fetchall()]
                    if len(cells) > 0:
                        for cell in cells: RPCq.put(Work(cell,cell))
                    qqrfill = len(cells); qqtfill += qqrfill
                    log.debug('Refilled Queue. (%d cells)' % qqrfill)
                
                if not killswitch and self.fail > self.config.trsh:
                    killswitch = None
                    log.error('Errors! Shutting down...')
                
                if not killswitch and self.pos >= qqtot:
                    killswitch = None
                    log.info('Work done. Stopping threads...')
                                
                if killswitch is None:
                    RPCq.put(PoisonPill(broadcast=True))
                    MapQ.put(PoisonPill(broadcast=True))
                    SQLq.put(PoisonPill())
                    killswitch = True
                
                if killswitch and doneQ.empty(): dontkillme = False
                
                    
                sleep(1); log.debug('Ping.') # I'm still alive
            
            except (KeyboardInterrupt): killswitch = None; raise KeyboardInterrupt
        

        log.info('Waiting for threads to exit or Timeout (90s)')
        
        for Minion in Minions:
            Minion.join(15)
            
        MapT.join(30)
        DBt.join(45)
        
if __name__ == '__main__':
    main()
