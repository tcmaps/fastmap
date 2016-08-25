fastmap
=======

The *fast* PoGo map.

##Featurette & ToDo:

- [x] map bootstrap data generator
- [x] standardized database structure
- [x] forts, gyms, park spawn points
- [ ] urban spawnpoints
- [ ] spawn point timing classification
- [ ] threading / multi accounts
- [ ] GUI / (live) map display

### bootstrap.py Usage:

#### Options

    bootstrap.py [-h] [-a AUTH_SERVICE] [-u USERNAME] [-p PASSWORD]
                      [-l LOCATION] [-r RADIUS] [-w WIDTH] [-f DBFILE]
                      [--level LEVEL] [-t DELAY] [-d] [-n]

#### Examples

    bootstrap.py -l "Area 51" -r 10000
... scans a 10,000 meters radius circle

bootstrap.py -l "37.235, -115.811" -w 10000
... scans a 10,000 x 10,000m square

> Written with [StackEdit](https://stackedit.io/).
