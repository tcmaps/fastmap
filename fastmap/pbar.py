#!/usr/bin/env python
import logging
import tqdm

class TqdmLogHandler (logging.Handler):
    def __init__ (self, level = logging.NOTSET):
        super (self.__class__, self).__init__ (level)

    def emit (self, record):
        try:
            msg = self.format (record)
            tqdm.tqdm.write (msg)
            self.flush ()
        except (KeyboardInterrupt, SystemExit):
            raise KeyboardInterrupt
        except:
            self.handleError(record)   