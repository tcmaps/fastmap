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
import sys
import json
import time
import sqlite3
import logging
import argparse

from pgoapi import PGoApi
from s2sphere import CellId, LatLng

log = logging.getLogger(__name__)

import utils

sys.path.append(os.path.dirname(os.path.realpath(__file__)))

def init_config():
    parser = argparse.ArgumentParser()
    config_file = "config.json"

    # If config file exists, load variables from json
    load   = {}
    if os.path.isfile(config_file):
        with open(config_file) as data:
            load.update(json.load(data))

    # Read passed in Arguments
    parser.add_argument("-a", "--auth_service", help="Auth Service ('ptc' or 'google')", default="ptc")
    parser.add_argument("-u", "--username", help="Username")
    parser.add_argument("-p", "--password", help="Password")
    parser.add_argument("-l", "--location", help="Location")
    parser.add_argument("-r", "--offset", help="rectangle size", default=1000, type=int)
    parser.add_argument("-t", "--delay", help="rpc request interval", default=10, type=int)
    parser.add_argument("-d", "--debug", help="Debug Mode", action='store_true', default=0)    
    config = parser.parse_args()
    utils.check_db()
    
    if config.location:
        utils.init_db(config.location, int(config.offset), 13);
    
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
  
    api.set_authentication(provider = config.auth_service, username = config.username, password =  config.password)
    api.activate_signature(utils.set_lib())    
    log.info ('API online! Scan starts in 5sec...')
    time.sleep(5)
    
    db = sqlite3.connect('db.sqlite')
    db_cur = db.cursor()
    db_cur.execute("SELECT cell_id FROM 'queque' ORDER BY cell_id")
    _tstats = [0, 0, 0, 0]
    
    scan_queque = [x[0] for x in db_cur.fetchall()]
    # http://stackoverflow.com/questions/3614277/how-to-strip-from-python-pyodbc-sql-returns
    
    if len(scan_queque) == 0: log.info('Nothing to scan!'); return
        
    for queq in scan_queque:    
                
        cell_ids = []
        _content = 0
        _tstats[0] += 1
        _cstats = [0, 0, 0]
        
        log.info('Scan {} of {}...'.format(_tstats[0],(len(scan_queque))))
        
        cell = CellId(queq)
        _ll = CellId.to_lat_lng(cell)
        lat, lng, alt = _ll.lat().degrees, _ll.lng().degrees, 0
        
        cell_ids = utils.cell_childs_2(cell)
        
        timestamps = [0,] * len(cell_ids)
        response_dict = []
        
        _try=1
        while _try:
            _try=0     
            
            try:
                api.set_position(lat, lng, alt)
                response_dict = api.get_map_objects(latitude=lat, longitude=lng, since_timestamp_ms = timestamps, cell_id = cell_ids)
                if 'status' in response_dict['responses']['GET_MAP_OBJECTS']:
                    if response_dict['responses']['GET_MAP_OBJECTS']['status'] == 1:
                        _try=0
            except:
                log.error(sys.exc_info()[0])
                time.sleep(10)
                _try=1
                
        for _map_cell in response_dict['responses']['GET_MAP_OBJECTS']['map_cells']:                        

            if 'forts' in _map_cell:
                for _frt in _map_cell['forts']:
                    if 'gym_points' in _frt:
                        _cstats[0]+=1
                        _type = 0
                        _content = utils.set_bit(_content, 2)
                        db_cur.execute("REPLACE INTO forts (fort_id, cell_id, pos_lat, pos_lng, fort_enabled, fort_type) "
                        "VALUES ('{}',{},{},{},{},{})".format(_frt['id'],_map_cell['s2_cell_id'],_frt['latitude'],_frt['longitude'], \
                        int(_frt['enabled']),0))
                    else:
                        _type = 1; _cstats[1]+=1
                        _content = utils.set_bit(_content, 1)
                        db_cur.execute("REPLACE INTO forts (fort_id, cell_id, pos_lat, pos_lng, fort_enabled, fort_type) "
                        "VALUES ('{}',{},{},{},{},{})".format(_frt['id'],_map_cell['s2_cell_id'],_frt['latitude'],_frt['longitude'], \
                        int(_frt['enabled']),1))
                                                             
            if 'spawn_points' in _map_cell:
                _content = utils.set_bit(_content, 0)
                for _spwn in _map_cell['spawn_points']:
                    _cstats[2]+=1;
                    spwn_id = CellId.from_lat_lng(LatLng.from_degrees(_spwn['latitude'],_spwn['longitude'])).parent(20).to_token()
                    db_cur.execute("REPLACE INTO spawns (spawn_id, cell_id, pos_lat, pos_lng) "
                    "VALUES ('{}',{},{},{})".format(spwn_id,_map_cell['s2_cell_id'],_spwn['latitude'],_spwn['longitude']))
            if 'decimated_spawn_points' in _map_cell:
                _content = utils.set_bit(_content, 0)
                for _spwn in _map_cell['decimated_spawn_points']:
                    _cstats[2]+=1;
                    spwn_id = CellId.from_lat_lng(LatLng.from_degrees(_spwn['latitude'],_spwn['longitude'])).parent(20).to_token()
                    db_cur.execute("REPLACE INTO spawns (cell_id, pos_lat, pos_lng) "
                    "VALUES ({},{},{})".format(_map_cell['s2_cell_id'],_spwn['latitude'],_spwn['longitude']))
            if 'wild_pokemons' in _map_cell:
                _content = utils.set_bit(_content, 0)
                #for _spwn in _map_cell['wild_pokemons']:
                    #_cstats[2]+=1;
                    #db_cur.execute("REPLACE INTO spawns (spawn_id, cell_id, pos_lat, pos_lng) "
                    #"VALUES ('{}',{},{},{})".format(_spwn['spawn_point_id'],_map_cell['s2_cell_id'],_spwn['latitude'],_spwn['longitude']))
					
            db_cur.execute("REPLACE INTO cells (cell_id, content, quick_scan) VALUES ({}, {}, {})".format(_map_cell['s2_cell_id'],_content,1))
			
        _tstats[1] += _cstats[0]; _tstats[2] += _cstats[1]; _tstats[3] += _cstats[2]
        db_cur.execute("DELETE FROM queque WHERE cell_id={}".format(cell.id()))
        db.commit()
        log.info("UPSERTed {} Gyms, {} Pokestops, {} Spawns. Sleeping...".format(*_cstats))
        time.sleep(int(config.delay))

    log.info('Scanned {} cells; got {} Gyms, {} Pokestops, {} Spawns'.format(*_tstats))

if __name__ == '__main__':
    main()
