import time, logging, sqlite3

from time import sleep
from s2sphere import CellId, LatLng
from sqlite3 import IntegrityError, ProgrammingError, DataError
from sqlite3 import OperationalError, InterfaceError, DatabaseError

from fastmap.core import Work, Minion, Poison
from fastmap.apiwrap import api_init, check_reponse, AccountBannedException
from fastmap.utils import set_bit, get_cell_ids, sub_cells_normalized
from pgoapi.exceptions import NotLoggedInException

log = logging.getLogger(__name__)


class MapRequester(Minion):
    
    def preinit(self):
        self.name = '[Req]'
    
    def runfirst(self):
        
        self.api = api_init(self.args)
        if self.api == None:   
            self.log.error(self.name + 'Failed to initialize PoGoAPI!')
            self.runs = False; return

        self.log.info("Online with account '%s' " % self.args.username)
        sleep(3)
    
    def main(self):
        
        log = self.log
        work = self.work
        
        if work.index is None: log.error('No work index'); return
        elif work.work is None: log.error('No work for id %d' % work.index); return
                    
        log.debug(self.name + ' does Cell %s.' % (work.index))         
        cell = CellId.from_token(work.work); alt=0
        lat = CellId.to_lat_lng(cell).lat().degrees 
        lng = CellId.to_lat_lng(cell).lng().degrees
        cell_ids = get_cell_ids(sub_cells_normalized(cell, level=15))
        
        timestamps = [0,] * len(cell_ids)
        self.api.set_position(lat, lng, alt)
        trys = self.config.retrys
        
        try: 
            while trys:
    
                response_dict = check_reponse(self.api.get_map_objects(\
                latitude=lat, longitude=lng, since_timestamp_ms = timestamps, cell_id = cell_ids))
                
                if response_dict is not None: break
                trys -= 1; sleep(self.config.delay)

        except AccountBannedException:
            log.critical('Worker %d down: Banned' % self.threadID)
            self.runs=False; return
        except NotLoggedInException:                
            self.api = None
            self.api = api_init(self.config)
        except Exception as e:
            self.log.error(e); return         
        else:
            if len(response_dict) > 0:
                self.output.put(Work(work.index,response_dict))
        
        finally:
            if response_dict is None:
                self.input.put(work)
        
        time.sleep(self.config.delay)

            
class MapParser(Minion):
      
    def preinit(self):
        self.name = '[Map]'
      
    def main(self):
        
        log = self.log
        work = self.work
        
        if work.work is None or len(work.work) == 0:
            self.output.put(Work(work.index, [], status=3)); return
        
        stats = [0, 0, 0, 0]
        querys = []
        
        for map_cell in work.work['responses']['GET_MAP_OBJECTS']['map_cells']:
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

        log.debug(' got {} Cells, {} Gyms, {} Pokestops, {} Spawns.'.format(*stats))
    

        if stats[0] == 0:
            self.output.put(Work(work.index, querys, status=3))
        elif (stats[1] + stats[2] + stats[3]) > 0:
            self.output.put(Work(work.index, querys, status=1))
        else:
            self.output.put(Work(work.index, querys, status=2))

        
class DataBaser(Minion):
    
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
        
        if len(work.work) > 0:

            try:
                self.lock.acquire()     
                for query in work.work:
                    dbc.execute(query)
            
            except (IntegrityError, ProgrammingError, DataError): 
                self.db.rollback()
                self.output.put(Work(work.index,False)); sleep(1)
                log.error('Upsert for Work %s failed!' % work.index)
            
            except (OperationalError, InterfaceError, DatabaseError):
                self.output.put(Poison())
                self.runs = False
                return # Shut down DB
            
            else:
                try:
                    if work.status == 1:
                        dbc.execute("UPDATE _queue SET scan_status=1 WHERE cell_id='%s'" % work.index)
                    elif work.status == 2:
                        dbc.execute("UPDATE _queue SET scan_status=2 WHERE cell_id='%s'" % work.index)
                        log.warning('Work %s returned empty!' % work.index)
                    elif work.status == 3:
                        dbc.execute("UPDATE _queue SET scan_status=3 WHERE cell_id='%s'" % work.index)
                        log.warning('Work %s failed!' % work.index)
                    self.db.commit()
                
                except (IntegrityError, ProgrammingError, DataError): 
                    self.db.rollback()
                    self.output.put(Work(work.index,False))
                    log.error('Commit failed!' % work.index)
                
                except (OperationalError, InterfaceError, DatabaseError):
                    self.output.put(Work(work.index,False))
                    self.output.put(Poison())
                    self.runs = False
                    return # Shut down DB
                
                else:
                    self.output.put(Work(work.index,True))
                    log.debug('Inserted %d querys.' % len(work.work))
            
            finally: self.lock.release()
        
        else:
            self.output.put(Work(work.index,False))
