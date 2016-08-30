import logging
import sqlite3

from s2sphere import CellId, LatLng
from fastmap.utils import set_bit
log = logging.getLogger(__name__)

def process_response(db, response_dict):
    
    stats = [0, 0, 0]
    dbc = db.cursor()
    
    if response_dict:
        if 'responses' in response_dict:
            for map_cell in response_dict['responses']['GET_MAP_OBJECTS']['map_cells']:
                cellid = CellId(map_cell['s2_cell_id']).to_token()
                content = 0                      
                
                if 'forts' in map_cell:
                    for fort in map_cell['forts']:
                        if 'gym_points' in fort:
                            stats[0]+=1
                            content = set_bit(content, 2)
                            dbc.execute("INSERT OR IGNORE INTO forts (fort_id, cell_id, pos_lat, pos_lng, fort_enabled, fort_type, last_scan) "
                            "VALUES ('{}','{}',{},{},{},{},{})".format(fort['id'],cellid,fort['latitude'],fort['longitude'], \
                            int(fort['enabled']),0,int(map_cell['current_timestamp_ms']/1000)))
                        else:
                            stats[1]+=1
                            content = set_bit(content, 1)
                            dbc.execute("INSERT OR IGNORE INTO forts (fort_id, cell_id, pos_lat, pos_lng, fort_enabled, fort_type, last_scan) "
                            "VALUES ('{}','{}',{},{},{},{},{})".format(fort['id'],cellid,fort['latitude'],fort['longitude'], \
                            int(fort['enabled']),1,int(map_cell['current_timestamp_ms']/1000)))
                                                                 
                if 'spawn_points' in map_cell:
                    content = set_bit(content, 0)
                    for spawn in map_cell['spawn_points']:
                        stats[2]+=1;
                        spwn_id = CellId.from_lat_lng(LatLng.from_degrees(spawn['latitude'],spawn['longitude'])).parent(20).to_token()
                        dbc.execute("INSERT OR IGNORE INTO spawns (spawn_id, cell_id, pos_lat, pos_lng, last_scan) "
                        "VALUES ('{}','{}',{},{},{})".format(spwn_id,cellid,spawn['latitude'],spawn['longitude'],int(map_cell['current_timestamp_ms']/1000)))
                if 'decimated_spawn_points' in map_cell:
                    content = set_bit(content, 0)
                    for spawn in map_cell['decimated_spawn_points']:
                        stats[2]+=1;
                        spwn_id = CellId.from_lat_lng(LatLng.from_degrees(spawn['latitude'],spawn['longitude'])).parent(20).to_token()
                        dbc.execute("INSERT OR IGNORE INTO spawns (spawn_id, cell_id, pos_lat, pos_lng, last_scan) "
                        "VALUES ('{}','{}',{},{},{})".format(spwn_id,cellid,spawn['latitude'],spawn['longitude'],int(map_cell['current_timestamp_ms']/1000)))
                        
                dbc.execute("INSERT OR IGNORE INTO cells (cell_id, content, last_scan) "
                "VALUES ('{}', {}, {})".format(cellid,content,int(map_cell['current_timestamp_ms']/1000)))
                log.debug(' got {} Gyms, {} Pokestops, {} Spawns.'.format(*stats))
                #self.stats[0] += stats[0]; self.stats[1] += stats[1]; self.stats[2] += stats[2]
    
            return stats
    
    return None

            
def pop_que(db, que):
    
    db.cursor().execute("DELETE FROM _queue WHERE cell_id='{}'".format(que)); db.commit()                      
