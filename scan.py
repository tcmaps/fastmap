#!/usr/bin/env python
"""
based on: pgoapi - Pokemon Go API
Copyright (c) 2016 tjado <https://github.com/tejado>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE
OR OTHER DEALINGS IN THE SOFTWARE.

Author: tjado <https://github.com/tejado>
        TC    <reddit.com/u/Tr4sHCr4fT>
"""

import os
import re
import sys
import json
import time
import struct
import sqlite3
import logging
import requests
import argparse

from pgoapi import PGoApi
from pgoapi.utilities import f2i, h2f
from pgoapi import utilities as util
from google.protobuf.internal import encoder
from geopy.geocoders import GoogleV3
from s2sphere import Cell, CellId, LatLng
import s2sphere

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
    parser.add_argument("-a", "--auth_service", help="Auth Service ('ptc' or 'google')", default="ptc")
    parser.add_argument("-u", "--username", help="Username")
    parser.add_argument("-p", "--password", help="Password")
    parser.add_argument("-r", "--cells", help="Cells to walk", default=20)
    parser.add_argument("-t", "--delay", help="get_map_objects refresh interval", default=10)
    parser.add_argument("-d", "--debug", help="Debug Mode", action='store_true', default=0)
    config = parser.parse_args()

    # Passed in arguments shoud trump
    for key in config.__dict__:
        if key in load and config.__dict__[key] == None:
            config.__dict__[key] = load[key]

    if config.auth_service not in ['ptc', 'google']:
      log.error("Invalid Auth service specified! ('ptc' or 'google')")
      return None

    return config

def main():
    # log settings
    # log format
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(module)10s] [%(levelname)5s] %(message)s')
    # log level for http request class
    logging.getLogger("requests").setLevel(logging.WARNING)
    # log level for main pgoapi class
    logging.getLogger("pgoapi").setLevel(logging.WARNING)
    # log level for internal pgoapi class
    logging.getLogger("rpc_api").setLevel(logging.WARNING)

    config = init_config()
    if not config:
        return

    if config.debug:
        logging.getLogger("requests").setLevel(logging.DEBUG)
        logging.getLogger("pgoapi").setLevel(logging.DEBUG)
        logging.getLogger("rpc_api").setLevel(logging.DEBUG)
        
    api = PGoApi()
    api.set_position(0,0,0)
    RPC_DELAY = config.delay
 
    while not api.login(config.auth_service, config.username, config.password):
        log.warning('Login failed! retrying in 3sec...')
        time.sleep(3)
 
    api.get_player()
    response_dict = api.call()
    log.info ('API online! Scan starts in 5sec...')
    time.sleep(5)
    
    db = sqlite3.connect('db.sqlite')
    db_cur = db.cursor()
    
    _tstats = [0, 0, 0, 0]
    
    run=1
    while run:
        
        db_cur.execute("SELECT cell_id FROM 'cells' WHERE quick_scan=0 ORDER BY cell_id LIMIT 0,{}".format(config.cells))
        # http://stackoverflow.com/questions/3614277/how-to-strip-from-python-pyodbc-sql-returns
        cell_ids = [x[0] for x in db_cur.fetchall()]
        
        if not len(cell_ids): break
        
        _tstats[0] += len(cell_ids)
        _cstats = [0, 0, 0]
        
        log.info('Scanning {} cells from {} to {}'.format(len(cell_ids),cell_ids[0],cell_ids[len(cell_ids)-1]))
        
        _ll = CellId.to_lat_lng(CellId(cell_ids[int(len(cell_ids)/2)]))
        lat, lng, alt = _ll.lat().degrees, _ll.lng().degrees, 0
        timestamps = [0,] * len(cell_ids)
        response_dict = []
        
        _try=1
        while _try:
            _try=0     
            
            try:
                api.set_position(lat, lng, alt)
                api.get_map_objects(latitude = util.f2i(lat), longitude = util.f2i(lng), since_timestamp_ms = timestamps, cell_id = cell_ids)
                response_dict = api.call()
                if 'status' in response_dict['responses']['GET_MAP_OBJECTS']:
                    if response_dict['responses']['GET_MAP_OBJECTS']['status'] == 1:
                        _try=0
            except:
                 print(sys.exc_info()[0])
                 time.sleep(10)
                 _try=1
                
        for _map_cell in response_dict['responses']['GET_MAP_OBJECTS']['map_cells']:                        
            _content = 0
            if 'forts' in _map_cell:
                _content=2
                for _frt in _map_cell['forts']:
                    if 'gym_points' in _frt:
                        _cstats[0]+=1
                        _type, _content = 0 , 6
                        db_cur.execute("REPLACE INTO forts (fort_id, cell_id, pos_lat, pos_lng, fort_enabled, fort_type) "
                        "VALUES ('{}',{},{},{},{},{})".format(_frt['id'],_map_cell['s2_cell_id'],_frt['latitude'],_frt['longitude'], \
                        int(_frt['enabled']),0))
                    else:
                        _type = 1; _cstats[1]+=1
                        db_cur.execute("REPLACE INTO forts (fort_id, cell_id, pos_lat, pos_lng, fort_enabled, fort_type) "
                        "VALUES ('{}',{},{},{},{},{})".format(_frt['id'],_map_cell['s2_cell_id'],_frt['latitude'],_frt['longitude'], \
                        int(_frt['enabled']),1))
                                                             
            if 'spawn_points' in _map_cell:
                _content+=1
                for _spwn in _map_cell['spawn_points']:
                    _cstats[2]+=1;
                    db_cur.execute("REPLACE INTO spawns (cell_id, pos_lat, pos_lng) "
                    "VALUES ({},{},{})".format(_map_cell['s2_cell_id'],_spwn['latitude'],_spwn['longitude']))
            
            db_cur.execute("UPDATE cells SET quick_scan=1, content={} WHERE cell_id={}".format(_content,_map_cell['s2_cell_id']))
            
        db.commit()
        
        _tstats[1] += _cstats[0]; _tstats[2] += _cstats[1]; _tstats[3] += _cstats[2]
        log.info("UPSERTed {} Gyms, {} Pokestops, {} Spawns".format(*_cstats))
        log.info ('Sleeping... ({}sec)'.format(RPC_DELAY))
        time.sleep(RPC_DELAY)
            
    log.info('Scanned {} cells; got {} Gyms, {} Pokestops, {} Spawns'.format(*_tstats))

if __name__ == '__main__':
    main()