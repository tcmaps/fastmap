#!/usr/bin/env python

import sys, sqlite3

if __name__ == '__main__':
    
    dbfilename = 'db.sqlite'
    if len(sys.argv) > 1: dbfilename = sys.argv[1]
    
    with sqlite3.connect(dbfilename) as db:
        
        db.cursor().execute("VACUUM")
        
    print('Donezo!')