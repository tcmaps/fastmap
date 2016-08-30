fastmap
=======

The *fast* PoGo map.   
  
![Alt Text]https://github.com/Tr4sHCr4fT/fastmap/blob/master/fastmap.gif
  
##Featurette & ToDo:   

- [x] *fast* bootstrap data generator
- [x] standardized database structure  
- [x] forts, gyms, park spawn points   
- [x] threading / multi accounts     
- [ ] scanning for urban spawnpoints  
- [ ] spawn times classification
- [ ] GUI / (live) map display

### bootstrap.py Usage:

#### Options

    bootstrap.py [-h] [-a AUTH_SERVICE] [-u USERNAME] [-p PASSWORD]
                      [-l LOCATION] [-r RADIUS] [-w WIDTH] [-f DBFILE]
                      [-m --minions THREADS] [--level LEVEL] [-t DELAY] [-d]

#### Examples

    bootstrap.py -l "Area 51" -w 9000
... scans a 9,000 x 9,000 m square

    bootstrap.py -l "37.235, -115.811" -r 9000
... scans a circle with 9km radius 






