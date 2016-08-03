-- version info
CREATE TABLE _config
(
version varchar(3) DEFAULT '1.0'
);

-- map cells
CREATE TABLE cells
(
cell_id BIGINT UNSIGNED PRIMARY KEY, -- s2sphere format cell id, uint64
skip BOOLEAN DEFAULT 0, -- skip scanning this cell?
quick_scan BOOLEAN DEFAULT 0, -- added visible forts, gyms and spawn locations?
full_scan BOOLEAN DEFAULT 0, -- cell fully traversed each 15min time quadrant?
content BIT(4), -- has (reserved);gyms;pokestops;spawns?
last_scan TIMESTAMP -- last scan/update time of this data
);

CREATE TABLE forts -- pokestops and gyms
(
fort_id VARCHAR(64), -- fort id
cell_id BIGINT UNSIGNED, -- s2 level 15 cell id containing this object
pos_s2 BIGINT UNSIGNED, -- s2 level 30 cell id for single value coordinate
pos_lat DOUBLE, -- wgs84 latitude coordinate stored in iee754 standard
pos_lng DOUBLE, -- wgs84 longtitude coordinate stored in iee754 standard
fort_enabled BOOLEAN, -- is fort enabled?
fort_type TINYINT UNSIGNED, -- gym or pokestop?
fort_description TEXT, -- (optional) fort description text
fort_image BLOB, -- (optional) fort image, base64 encoded
fort_sponsor TINYTEXT, -- (optional) fort sponsor? (not used yet)
fort_last_modified TIMESTAMP, -- (optional) some internal modify time
last_scan TIMESTAMP, -- last scan/update time of this data
FOREIGN KEY(cell_id) REFERENCES cells(cell_id),
PRIMARY KEY (pos_lat, pos_lng, fort_id)
);

CREATE TABLE spawns -- spawn points
(
spawn_id VARCHAR(64) DEFAULT '0', -- spawn point id
cell_id BIGINT UNSIGNED, -- s2 level 15 cell id containing this object
pos_s2 BIGINT UNSIGNED, -- s2 level 30 cell id for single value coordinate
pos_lat DOUBLE, -- wgs84 latitude coordinate stored in iee754 standard
pos_lng DOUBLE, -- wgs84 longtitude coordinate stored in iee754 standard
static_spawner SMALLINT UNSIGNED, -- is this spawn spawning always the same pokemon? 0 if not
spawn_time_base TIME DEFAULT 0, -- the minutes after midnight the spawn spawns
spawn_time_offset TIME DEFAULT 0, -- minutes until respawn (30,60...180?)
spawn_time_dur TIME DEFAULT 0, -- spawn duration aka time_till_hidden
last_scan TIMESTAMP, -- last scan/update time of this data
FOREIGN KEY(cell_id) REFERENCES cells(cell_id),
PRIMARY KEY (pos_lat, pos_lng, spawn_id)
);
