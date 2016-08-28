import logging

log = logging.getLogger(__name__)


class Work():
    def __init__(self, reference, content):
        self.index = reference      
        self.work = content