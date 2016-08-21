#!/usr/bin/env python

import os
import sqlite3
import s2sphere 

from geopy.geocoders import GoogleV3
from geographiclib.geodesic import Geodesic
from s2sphere import Cell, CellId, LatLng


def set_lib():
    if os.name =='nt':
        return 'encrypt.dll'
    elif os.name =='posix':
        return 'libencrypt.so'

def check_db():
    if os.path.isfile('db.sqlite'):
        return True
    else:
        if os.name =='nt':
            os.system('copy temp.db db.sqlite')
        elif os.name =='posix':
            os.system('cp temp.db db.sqlite')
    return False

def init_db(location, offset, level):
    
    lat, lng, alt = get_pos_by_name(location)
    db = sqlite3.connect('db.sqlite')
    
    r = s2sphere.RegionCoverer()
    r.min_level, r.min_level = level, level
    g1 = Geodesic.WGS84.Direct(lat, lng, (360-45), offset)
    p1 = s2sphere.LatLng.from_degrees(g1['lat2'],g1['lon2'])
    g2 = Geodesic.WGS84.Direct(lat, lng, (180-45), offset)
    p2 = s2sphere.LatLng.from_degrees(g2['lat2'],g2['lon2'])
    cell_ids = r.get_covering(s2sphere.LatLngRect.from_point_pair(p1, p2))

    cells=0    
    for cell in cell_ids:
        if cell.level() == level:
            db.cursor().execute("REPLACE INTO queque (cell_id) VALUES ({})".format(cell.id()))
            cells+=1
    db.commit()
    
    return len(cells)

def get_pos_by_name(location_name):
    geolocator = GoogleV3()
    loc = geolocator.geocode(location_name)
    if not loc:
        return None

    return (loc.latitude, loc.longitude, loc.altitude)

def set_bit(value, bit):
    return value | (1<<bit)

def cell_childs_2(cell_san):
    cells = []
    for x in range(4):
        chibi_cell = cell_san.child(x)
        for y in range(4):
            cells.append(chibi_cell.child(y).id())    
    return sorted(cells)

def get_cell_walk(lat, long, radius):
    origin = CellId.from_lat_lng(LatLng.from_degrees(lat, long)).parent(15)
    walk = [origin.id()]
    right = origin.next()
    left = origin.prev()

    # Search around provided radius
    for i in range(radius):
        walk.append(right.id())
        walk.append(left.id())
        right = right.next()
        left = left.prev()

    # Return everything
    return sorted(walk)

def north(cell):
    return cell.get_edge_neighbors()[0]
def east(cell):
    return cell.get_edge_neighbors()[3]
def south(cell):
    return cell.get_edge_neighbors()[2]
def west(cell):
    return cell.get_edge_neighbors()[1]
