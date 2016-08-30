#!/usr/bin/env python
VERSION = '2.1'

"""
based on: pgoapi - Pokemon Go API
Copyright (c) 2016 tjado <https://github.com/tejado>

Author: TC    <reddit.com/u/Tr4sHCr4fT>
Version: 1.5
"""

import os, sys, json
import argparse, logging
import sqlite3

from time import sleep
from pkgutil import find_loader
from s2sphere.sphere import CellId, LatLng
from sqlite3 import IntegrityError, ProgrammingError, DataError
from sqlite3 import OperationalError, InterfaceError, DatabaseError

from pgoapi.exceptions import NotLoggedInException
from fastmap.db import check_db, fill_db
from fastmap.apiwrap import api_init, get_response, AccountBannedException
from fastmap.utils import get_accounts, cover_circle, cover_square, get_cell_ids, sub_cells_normalized, set_bit

log = logging.getLogger(__name__)


def init_config():
    parser = argparse.ArgumentParser()     
    logging.basicConfig(level=logging.INFO,\
                        format='[%(levelname)5s] %(asctime)s %(message)s', datefmt="%d.%m.%y %H:%M:%S")
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
    parser.add_argument("--maxq", help="maximum queue per worker", default=500, type=int)
    parser.add_argument("--pbar", help="tqdm progressbar", action='store_true', default=1)
    parser.add_argument("-t", "--delay", help="rpc request interval", default=10, type=int)
    parser.add_argument("-m", "--minions", help="thread / worker count", default=10, type=int)
    parser.add_argument("-v", "--verbose", help="Verbose Mode", action='store_true', default=0)    
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
        log.info('Added %d items to scan queue.' % fill_db(config.dbfile, cells))
        del cells, lat, lng
    
    if config.minions < 1: config.minions = 1
    
    if config.pbar:
        config.pbar = find_loader('tqdm')
        if config.pbar is not None: config.pbar = True
        else: log.warning("'pip install tqdm' to see a fancy progress bar!")
    
    if os.path.isfile('DEBUG'): logging.getLogger(__name__).setLevel(logging.DEBUG)
    
    return config

def main():
    
    config = init_config()
    if not config:
        log.error('Configuration Error!'); return
    
    bar = dummybar()
    minions = config.minions
    db = sqlite3.connect(config.dbfile)
    log.info('DB loaded.')
    totalwork  = db.cursor().execute("SELECT COUNT(*) FROM _queue").fetchone()[0]
    
    # some sanity checks   
    if totalwork == 0: log.info('Nothing to scan!'); return
    
    if not os.path.isfile(config.accfile):
        minions = 1; accounts = [config]
    else:
        accounts = get_accounts(config.accfile)
        if len(accounts) < config.minions: minions = len(accounts)

    if totalwork < minions: minions = totalwork
    
    workpart = ( totalwork / minions )
    if workpart > config.maxq : workpart = config.maxq
    
# all OK?
    done = 0
    try:
# initialize APIs
        workers = []
        for m in xrange(minions):
            log.info('Initializing worker %2d of %2d' % (m,minions))
            api = api_init(accounts[m]); sleep(3)
            if api is not None:
                workers.append(api)
                log.info("Logged into  '%s'" % accounts[m].username)
            else: log.error("Login failed for  '%s'" % accounts[m].username)
        log.info('Workers:%3d' % len(workers))
# end worker init loop

# ETA
        n = (totalwork / len(workers))
        log.info('Total %5d cells, %3d Workers, %5d cells each.' % (totalwork, minions, n))
        ttot = (n * config.delay + 1); m, s = divmod(ttot, 60); h, m = divmod(m, 60)
        log.info('ETA %d:%02d:%02d' % (h, m, s)); del h,m,s, ttot, minions

# last chance to abort
        for i in xrange(3):
            log.info('Start in %d...' % (3-i)); sleep(1)
        log.info("Let's go!")

# init bar
        if config.pbar:
            import tqdm
            from fastmap.pbar import TqdmLogHandler
            bar = tqdm.tqdm(total=totalwork); log.addHandler(TqdmLogHandler())
            logging.getLogger(__name__).setLevel(logging.WARN)

# open DB
        with sqlite3.connect(config.dbfile) as db:  
            totalstats = [0, 0, 0, 0]  

## main loop        
            while done < totalwork and len(workers) > 0:
##
 
# kill some zombies
                for i in xrange(len(workers)):
                    if workers[i] is None:
                        workers[i].pop

# fetch DB        
                cells = [x[0] for x in db.cursor().execute("SELECT cell_id FROM '_queue' "
                            "ORDER BY cell_id LIMIT %d" % ((len(workers)))).fetchall()]

# RPC loop            
                responses = []
                delay = float( float(config.delay) / float(len(workers)) )
                for i in xrange(len(cells)):
                    
                    sleep(delay)    
                    
                    cell = CellId.from_token(cells[i])
                    lat = CellId.to_lat_lng(cell).lat().degrees 
                    lng = CellId.to_lat_lng(cell).lng().degrees
                    cell_ids = get_cell_ids(sub_cells_normalized(cell, level=15))
                    
                    log.debug('W%2d doing request for %s (%f, %f)' % (i,cells[i],lat,lng))
                    
                    try:
                        response_dict = get_response(workers[i], cell_ids, lat, lng)
                    except AccountBannedException:
                        workers[i] = None
                        log.error('Worker %d down: Banned' % accounts[i])
                    except NotLoggedInException:
                        sleep(config.delay / 2) 
                        workers[i] = None
                        workers[i] = api_init(accounts[i])
                        response_dict = get_response(workers[i], cell_ids, lat, lng)
                    except: log.error(sys.exc_info()[0]); sleep(config.delay) 
                    finally:
                        responses.append(response_dict); bar.update()                        
# end RP loop

# parse loop    
                querys = []
                for i in xrange(len(responses)):

                    stats = [0, 0, 0, 0]
                    response_dict = responses[i]
                    
                    if response_dict is None or len(response_dict) == 0: continue
                    
                    for map_cell in response_dict['responses']['GET_MAP_OBJECTS']['map_cells']:
                        cellid = CellId(map_cell['s2_cell_id']).to_token()
                        stats[0] += 1
                        content = 0                   
                        
                        if 'forts' in map_cell:
                            for fort in map_cell['forts']:
                                if 'gym_points' in fort:
                                    stats[1]+=1
                                    content = set_bit(content, 2)
                                    querys.append("INSERT OR IGNORE INTO forts (fort_id, cell_id, pos_lat, pos_lng, fort_enabled, fort_type, last_scan) "
                                    "VALUES ('{}','{}',{},{},{},{},{})".format(fort['id'],cellid,fort['latitude'],fort['longitude'], \
                                    int(fort['enabled']),0,int(map_cell['current_timestamp_ms']/1000)))
                                else:
                                    stats[2]+=1
                                    content = set_bit(content, 1)
                                    querys.append("INSERT OR IGNORE INTO forts (fort_id, cell_id, pos_lat, pos_lng, fort_enabled, fort_type, last_scan) "
                                    "VALUES ('{}','{}',{},{},{},{},{})".format(fort['id'],cellid,fort['latitude'],fort['longitude'], \
                                    int(fort['enabled']),1,int(map_cell['current_timestamp_ms']/1000)))
                                                                         
                        if 'spawn_points' in map_cell:
                            content = set_bit(content, 0)
                            for spawn in map_cell['spawn_points']:
                                stats[3]+=1;
                                spwn_id = CellId.from_lat_lng(LatLng.from_degrees(spawn['latitude'],spawn['longitude'])).parent(20).to_token()
                                querys.append("INSERT OR IGNORE INTO spawns (spawn_id, cell_id, pos_lat, pos_lng, last_scan) "
                                "VALUES ('{}','{}',{},{},{})".format(spwn_id,cellid,spawn['latitude'],spawn['longitude'],int(map_cell['current_timestamp_ms']/1000)))
                        if 'decimated_spawn_points' in map_cell:
                            content = set_bit(content, 0)
                            for spawn in map_cell['decimated_spawn_points']:
                                stats[3]+=1;
                                spwn_id = CellId.from_lat_lng(LatLng.from_degrees(spawn['latitude'],spawn['longitude'])).parent(20).to_token()
                                querys.append("INSERT OR IGNORE INTO spawns (spawn_id, cell_id, pos_lat, pos_lng, last_scan) "
                                "VALUES ('{}','{}',{},{},{})".format(spwn_id,cellid,spawn['latitude'],spawn['longitude'],int(map_cell['current_timestamp_ms']/1000)))
                                
                        querys.append("INSERT OR IGNORE INTO cells (cell_id, content, last_scan) "
                        "VALUES ('{}', {}, {})".format(cellid,content,int(map_cell['current_timestamp_ms']/1000)))
                    
                    log.debug('%s: ' % cells[i] + '%d Cells, %d Gyms, %d Pokestops, %d Spawns.' % tuple(stats))
                    totalstats[0] += stats[0]; totalstats[1] += stats[1]; totalstats[2] += stats[2]; totalstats[3] += stats[3]
                    querys.append("DELETE FROM _queue WHERE cell_id='{}'".format(cells[i]))
                    log.debug('Removing %s from Queue' % cells[i])
                    done += 1
# end parse loop                    
                    
# save to DB   #f = open('dump.sql','a')
                try:
                    dbc = db.cursor()
                    for query in querys:
                        dbc.execute(query) #;f.write(query); f.write(';\n')

                except (IntegrityError, ProgrammingError, DataError): 
                    db.rollback(); log.error('SQL Syntax Error');
                except (OperationalError, InterfaceError, DatabaseError):
                    log.critical('Database corrupted or locked'); return
                except KeyboardInterrupt: db.rollback(); raise KeyboardInterrupt
                else: db.commit(); log.debug('Inserted %d queries' % len(querys))
                 
# feedback                
                log.info('Queue: %5d done, %5d left' % (done,totalwork-done)); sleep(1)

## end main loop        
        
            log.info('Total: %5d Cells, %5d Gyms, %5d Pokestops, %5d Spawns.' % tuple(totalstats)) 

##
    except KeyboardInterrupt: log.info('Aborted!')
    else: print("Dekimashita!")
    finally: db.close(); bar.close()


class dummybar(object):
    def __init__(self): pass
    def close(self): pass
    def update(self, dummy): pass

if __name__ == '__main__':
    main()
