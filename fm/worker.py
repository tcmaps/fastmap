import sys, time, logging
import sqlite3

from threading import Thread
from s2sphere import CellId, LatLng

from fm.core import Work
from fm.apiwrap import api_init, get_response, Status3Exception
from fm.utils import set_bit, get_cell_ids, sub_cells_normalized
from pgoapi.exceptions import NotLoggedInException

log = logging.getLogger(__name__)


class Overseer(Thread):
    
    def __init__(self, threadID, config, worklist, params=None):
        Thread.__init__(self)
        self.threadID = threadID
        self.config = config
        self.parameters = params
        self.workload = worklist        
        self.pos = 0
        self.name = '(%2d)' % self.threadID


class RPCworker(Thread):
    
    def __init__(self, threadID, config, account, workin, workout, params=None):
        Thread.__init__(self)
        self.threadID = threadID
        self.config = config
        self.config.username = account.username
        self.config.password = account.password
        self.parameters = params
        self.workload = workin        
        self.results = workout
        self.pos = 0
        self.name = '(%2d)' % self.threadID
        
        self.api = api_init(self.config)
        if self.api == None:   
            log.error('Login failed!'); return
        log.info(self.name + " Logged into account '%s' " % self.config.username)
    
    def run(self):
        
        while self.workload:
            
            work = self.workload.get()
            
            log.info(self.name + ' Cell %d of %d.' % (self.pos+1,len(self.workload)))         
            cell = CellId.from_token(work.content)
            lat = CellId.to_lat_lng(cell).lat().degrees 
            lng = CellId.to_lat_lng(cell).lng().degrees
            cell_ids = get_cell_ids(sub_cells_normalized(cell, level=15))
            
            try:
                response_dict = get_response(self.api, cell_ids, lat, lng)
            except Status3Exception:
                ('Worker %d down: Banned' % self.threadID); return
            except NotLoggedInException:
                self.workload.put(work)                
                self.api = None
                self.api = api_init(self.config)
                continue
            except:
                self.log.error(sys.exc_info()[0]); return         
            else:
                self.result.put(Work(work.id,response_dict))
            finally:
                if response_dict is None:
                    self.workload.put(work)
                time.sleep(self.config.delay)

        return None

            
class MapWorker(Thread):
    
    def __init__(self, threadID, workin, workout, params=None):
        Thread.__init__(self)
        self.threadID = threadID
        self.parameters = params
        self.workload = workin        
        self.results = workout
        self.pos = 0
        self.name = '(%2d)' % self.threadID
        
    def run(self):
        
        while self.workload:
            
            work = self.workload.get()
            
            stats = [0, 0, 0]
            querys = []
            
            for map_cell in work.content['responses']['GET_MAP_OBJECTS']['map_cells']:
                cellid = CellId(map_cell['s2_cell_id']).to_token()
                content = 0                      
                
                if 'forts' in map_cell:
                    for fort in map_cell['forts']:
                        if 'gym_points' in fort:
                            stats[0]+=1
                            content = set_bit(content, 2)
                            querys.append("INSERT OR IGNORE INTO forts (fort_id, cell_id, pos_lat, pos_lng, fort_enabled, fort_type, last_scan) "
                            "VALUES ('{}','{}',{},{},{},{},{})".format(fort['id'],cellid,fort['latitude'],fort['longitude'], \
                            int(fort['enabled']),0,int(map_cell['current_timestamp_ms']/1000)))
                        else:
                            stats[1]+=1
                            content = set_bit(content, 1)
                            querys.append("INSERT OR IGNORE INTO forts (fort_id, cell_id, pos_lat, pos_lng, fort_enabled, fort_type, last_scan) "
                            "VALUES ('{}','{}',{},{},{},{},{})".format(fort['id'],cellid,fort['latitude'],fort['longitude'], \
                            int(fort['enabled']),1,int(map_cell['current_timestamp_ms']/1000)))
                                                                 
                if 'spawn_points' in map_cell:
                    content = set_bit(content, 0)
                    for spawn in map_cell['spawn_points']:
                        stats[2]+=1;
                        spwn_id = CellId.from_lat_lng(LatLng.from_degrees(spawn['latitude'],spawn['longitude'])).parent(20).to_token()
                        querys.append("INSERT OR IGNORE INTO spawns (spawn_id, cell_id, pos_lat, pos_lng, last_scan) "
                        "VALUES ('{}','{}',{},{},{})".format(spwn_id,cellid,spawn['latitude'],spawn['longitude'],int(map_cell['current_timestamp_ms']/1000)))
                if 'decimated_spawn_points' in map_cell:
                    content = set_bit(content, 0)
                    for spawn in map_cell['decimated_spawn_points']:
                        stats[2]+=1;
                        spwn_id = CellId.from_lat_lng(LatLng.from_degrees(spawn['latitude'],spawn['longitude'])).parent(20).to_token()
                        querys.append("INSERT OR IGNORE INTO spawns (spawn_id, cell_id, pos_lat, pos_lng, last_scan) "
                        "VALUES ('{}','{}',{},{},{})".format(spwn_id,cellid,spawn['latitude'],spawn['longitude'],int(map_cell['current_timestamp_ms']/1000)))
                        
                querys.append("INSERT OR IGNORE INTO cells (cell_id, content, last_scan) "
                "VALUES ('{}', {}, {})".format(cellid,content,int(map_cell['current_timestamp_ms']/1000)))

            log.debug(self.name + ' got {} Gyms, {} Pokestops, {} Spawns.'.format(*stats))
        
        for query in querys:
            self.results.put(Work(work.id,query)) 

        return None


class DBworker(Thread):
    
    def __init__(self, threadID, workin, workout, dbfile, dblock, params=None):
        Thread.__init__(self)
        self.threadID = threadID
        self.parameters = params
        self.workload = workin        
        self.results = workout
        self.pos = 0
        self.name = '(%2d)' % self.threadID
        
        self.db = sqlite3.connect(dbfile)
        self.lock = dblock 
        
    def run(self):
        
        works = []
        while self.workload: works.append(self.workload.get())
            
        with self.db.cursor() as dbc:
            self.lock.acquire()
            for work in works:
                try: dbc.execute(work.work)
                except:
                    self.log.error(sys.exc_info()[0])
                    self.results.put(Work(work.index,False))
                    continue
            
            try: self.db.commit()
            except: return
            finally: self.lock.release()

