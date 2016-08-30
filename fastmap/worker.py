import time, logging, sqlite3

from time import sleep
from s2sphere import CellId, LatLng
from sqlite3 import IntegrityError, ProgrammingError, DataError
from sqlite3 import OperationalError, InterfaceError, DatabaseError

from fastmap.core import Work, Minion, PoisonPill
from fastmap.apiwrap import api_init, get_response, Status3Exception
from fastmap.utils import set_bit, get_cell_ids, sub_cells_normalized
from pgoapi.exceptions import AuthException, NotLoggedInException

log = logging.getLogger(__name__)


class MapRequest(Minion):
    
    def preinit(self):
        self.name = '[Req]'
    
    def runfirst(self):
        
        self.api = api_init(self.parameters)
        if self.api == None:   
            self.log.error(self.name + 'Login failed!')
            raise AuthException
        
        self.log.info(self.name + " Logged into account '%s' " % self.parameters.username)
        
        sleep(3)
    
    def main(self):
        
        log = self.log
        work = self.work
            
        log.debug(self.name + ' does Cell %s.' % (work.index))         
        cell = CellId.from_token(work.work)
        lat = CellId.to_lat_lng(cell).lat().degrees 
        lng = CellId.to_lat_lng(cell).lng().degrees
        cell_ids = get_cell_ids(sub_cells_normalized(cell, level=15))
        
        try:
            response_dict = get_response(self.api, cell_ids, lat, lng)
        except Status3Exception:
            log.critical('Worker %d down: Banned' % self.threadID); return
        except NotLoggedInException:
            self.input.put(work)                
            self.api = None
            self.api = api_init(self.config)
        except Exception as e:
            self.log.error(e); return         
        else:
            self.output.put(Work(work.index,response_dict))
        
        finally:
            if response_dict is None:
                self.input.put(work)
        
        time.sleep(self.config.delay)

            
class MapParse(Minion):
      
    def preinit(self):
        self.name = '[Map]'
      
    def main(self):
        
        log = self.log
        work = self.work
        
        stats = [0, 0, 0]
        querys = []
        
        for map_cell in work.work['responses']['GET_MAP_OBJECTS']['map_cells']:
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
    
        self.output.put(Work(work.index,querys)) 

        
class DataBase(Minion):
    
    def preinit(self):
        self.name = '[DB]'
        
    def postinit(self):
        self.lock = self.locks[0]
    
    def runfirst(self):
        self.db = sqlite3.connect(self.config.dbfile)

    def main(self):
        
        log = self.log
        work = self.work
        
        dbc = self.db.cursor()
        
        # 100% empty cell?
        if len(work.work) == 0:
            work.work = ["UPDATE _queue SET scan_status=2 WHERE cell_id='%s'" % work.index]
            log.warning(self.name + ' Cell %s returned empty!' % work.index)
            
        try:
            self.lock.acquire()     
            for query in work.work:
                dbc.execute(query)
            dbc.execute("UPDATE _queue SET scan_status=1 WHERE cell_id='%s'" % work.index)
            self.db.commit()
        
        except (IntegrityError, ProgrammingError, DataError): 
            self.db.rollback()
            dbc.execute("UPDATE _queue SET scan_status=3 WHERE cell_id='%s'" % work.index)
            self.output.put(Work(work.index,False)); sleep(1)
            log.error(self.name + ' Upsert for Cell %s failed!' % work.index)
        
        except (OperationalError, InterfaceError, DatabaseError):
            self.output.put(PoisonPill())
            return # Shut down DB
        
        else:
            self.output.put(Work(work.index,True))                    
            log.debug(self.name + ' inserted %d Querys.' % len(work.work))
            
        finally: self.lock.release()