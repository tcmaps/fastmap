#!/usr/bin/env python

import logging
from threading import Thread


class Work():
    def __init__(self, reference, content):
        self.index = reference      
        self.work = content
        
class Params():
        def __init__(self, paramdict):
            self.params = paramdict      

class PoisonPill():
        def __init__(self, broadcast=False):
            self.relay = broadcast
            self.kill = True
            pass


class Mastermind(Thread):
    
    def __init__(self, threadID, config=None, worklist=None, params=None, signals=None, ):
        Thread.__init__(self)
        self.threadID = threadID
        self.name = '[Master %2d]' % self.threadID
        self.config = config
        self.parameters = params
        self.workload = worklist   
        self.control = signals     
        self.pos = 0
        self.log = logging.getLogger(self.name)
        
    def run(self):
        pass

class Minion(Thread):
    
    # generic constructor
    def __init__(self, threadID=0, config=None, workin=None, workout=None,\
                       params=None, locks=None, events=None, signals=None):
        
        Thread.__init__(self)
        self.threadID = threadID
        self.name = '[Minion %2d]' % self.threadID
        
        self.preinit()
        
        self.config = config
        self.parameters = params
        self.input = workin        
        self.output = workout
        self.control = signals
        self.locks = locks
        self.events = events
        self.work = None
        self.runs = 1
        self.pos = 0
        self.log = logging.getLogger(self.name)
        
        self.postinit()
    
    
    # generic start, run and stop handling
    def run(self):
        
        # run this once
        self.firstrun()
        
        while self.runs:
            
            # optionaly wait for events
            self.wait()
            
            # fetch queue
            self.work = self.input.get()
            
            # sanity check
            if self.work == None:
                continue
            
            # broadcast to other threads
            if hasattr(self.work, 'relay'):
                if self.work.relay is True:
                    self.input.put(self.work)
            
            # stop if received
            if hasattr(self.work, 'kill'):
                if self.work.kill is True:
                    self.runs = False
                    break

            #if self.work is PoisonPill:
            #    self.runs = False
            #    break
            
            # put back last work on error
            self.main()
            #except Exception as e:
                #self.input.put(self.work)
                #self.log.error(e)
                
        self.cleanup()
        
        return


    # override these to customize
    
    def preinit(self):
        pass    
    
    def postinit(self):
        pass

    def firstrun(self):
        pass
    
    def wait(self):
        pass

    def main(self):
        pass
    
    def cleanup(self):
        pass
