from logging import NullHandler
from ast import literal_eval
import io

class RequestCountHandler(NullHandler):

    def __init__(self,queue):
        NullHandler.__init__(self)
        self.queue = queue

    def handle(self, record):
        if False and "request" in record.msg:
            print("adding")
            dct = literal_eval(record.msg)
            self.queue.put(int(dct['requests']), block=True)

    def get_queue(self):
        return self.queue

    @property
    def request_count(self):
        return self.__request_count

