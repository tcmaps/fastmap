FMDBVERSION = 2.2
import sqlite3, os, logging
log = logging.getLogger(__name__)

def check_db(dbfile):
    
    if not os.path.isfile(dbfile):
        if not create_db(dbfile): return False
    
    with sqlite3.connect(dbfile) as db:
        try: version = db.cursor().execute("SELECT version FROM '_config'").fetchone()[0]
        except Exception as e: log.error(e); return False
        
    if version != FMDBVERSION:
        version = convert_db(dbfile, version)
    
    if version != FMDBVERSION:
        log.error("Database version mismatch! Expected {}, got {}...".format(FMDBVERSION,version))
        return False
    
    return True

def fill_db(dbfile, cells):
    with sqlite3.connect(dbfile) as db:
        counter=0    
        for cell in cells:
            db.cursor().execute("INSERT OR IGNORE INTO _queue (cell_id,cell_level,scan_status) "
                                "VALUES ('{}',{}, 0)".format(cell.to_token(),cell.level()))
            counter+=1
        db.commit()
    return counter

def convert_db(dbfile, olddbv):
    with sqlite3.connect(dbfile) as db: 
        try:
            newdbv = olddbv
            if newdbv == 2.1 or newdbv == '2.1' or newdbv == "'2.1'":
                log.info('Converting DB from 2.1 to 2.2...')
        
                db.cursor().execute("DROP TABLE _config")
                db.execute("CREATE TABLE _config (version DECIMAL DEFAULT 1.0)")              
                db.cursor().execute("ALTER TABLE _queue ADD scan_status INT")
                db.cursor().execute("UPDATE _queue SET scan_status = 0 WHERE scan_status IS NULL")
                db.execute("INSERT INTO _config (version) VALUES (2.2)")
                newdbv = 2.2
            
            db.cursor().execute("VACUUM")
        except Exception as e: log.error(e); db.rollback()
        else: db.commit()
        finally:
            if newdbv == FMDBVERSION:
                log.info("DB converted to {}".format(FMDBVERSION)); return newdbv
            else:
                log.error('DB could not be converted!'); return olddbv

def create_db(dbfile):
    try:
        db = sqlite3.connect(dbfile); dbc = db.cursor()
        dbc.execute("CREATE TABLE _config (version DECIMAL DEFAULT 1.0)")
        dbc.execute("CREATE TABLE _queue (cell_id VARCHAR PRIMARY KEY, cell_level INT, scan_status INT) WITHOUT ROWID")
        dbc.execute("CREATE TABLE cells (cell_id VARCHAR PRIMARY KEY, content INT, last_scan TIMESTAMP) WITHOUT ROWID")
        dbc.execute("CREATE TABLE forts (fort_id VARCHAR PRIMARY KEY, cell_id VARCHAR, \
        pos_lat DOUBLE, pos_lng DOUBLE, fort_enabled BOOLEAN, fort_type INT, fort_description TEXT, \
        fort_image BLOB, fort_sponsor TEXT, fort_last_modified TIMESTAMP, last_scan TIMESTAMP,\
        FOREIGN KEY ( cell_id ) REFERENCES cells (cell_id) ) WITHOUT ROWID")
        dbc.execute("CREATE TABLE spawns (spawn_id VARCHAR PRIMARY KEY, cell_id VARCHAR, \
        pos_lat DOUBLE, pos_lng DOUBLE, static_spawner INT DEFAULT (0), nest_spawner INT DEFAULT (0), \
        spawn_time_base TIME, spawn_time_offset TIME, spawn_time_dur TIME, last_scan TIMESTAMP, \
        FOREIGN KEY (cell_id) REFERENCES cells (cell_id) ) WITHOUT ROWID")
        dbc.execute("INSERT INTO _config (version) VALUES (%s)" % FMDBVERSION)
        log.info('DB created!')
    except Exception as e: log.error(e); db.rollback(); return False
    db.commit(); db.close()
    return True
        
if __name__ == '__main__':
    print check_db('db.sqlite')