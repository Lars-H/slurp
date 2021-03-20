'''

Created on Jul 14, 2020
Implements the Xnoptional operator.
The intermediate results are represented in a queue.
@author: Lars Heling

'''
from multiprocessing import Queue, Value
from Queue import Empty
from time import time
from operatorstructures import Record, RJTTail, Tuple
from random import randint

class Xgoptional(object):

    def __init__(self, id_operator, vars_left, vars_right, eddies, eddy=None):
        self.left_table = dict()
        self.right_table = dict()
        self.id_operator = id_operator
        self.eof = Tuple("EOF", 1, 1, set(), self.id_operator)

        self.eddies = eddies
        if eddy:
            self.eddy = eddy
        else:
            self.eddy = randint(1, self.eddies)
        self.bag = []
        self.left = None
        self.right = None
        self.qresults = None
        self.sources = None
        self.probing = Value('i', 1)
        self.independent_inputs = 2

        self.vars_left   = set(vars_left)
        self.vars_right  = set(vars_right)
        self.vars = self.vars_left.intersection(self.vars_right)

    def __str__(self):
        return str("Xgoptional")

    @staticmethod
    def symmetric():
        return True

    def execute(self, inputs, out):
        # Executes the Xgoptional.
        self.left = inputs[0]
        self.right = inputs[1]
        self.qresults = out

        tuple1 = None
        tuple2 = None

        # Initialize tuples.
        # Get the tuples from the queues
        while True:
            # Try to get and process tuple from left_plan queue.

            try:
                tuple1 = self.left.get(False)
                if not (tuple1.data == "EOF"):
                    self.bag.append(tuple1)
                self.stage1(tuple1, self.left_table, self.right_table, self.vars_right)

            except Empty:
                # Empty: in tuple2 = self.right_plan.get(False), when the queue is empty.
                # logging.info("Eddy {} Operator {} right_plan queue is empty".format(self.eddy ,self.id_operator))
                #self.probing.value = 0
                pass
            except TypeError:
                # TypeError: in resource = resource + tuple[var], when the tuple is "EOF".
                pass
            except IOError:
                # IOError: when a tuple is received, but the alarm is fired.
                pass

            # Try to get and process tuple from right_plan queue.
            try:
                tuple2 = self.right.get(False)
                #if not (tuple2.data == "EOF"):
                #    self.bag.append(tuple2 )
                self.stage1(tuple2, self.right_table, self.left_table, self.vars_left)

            except Empty:
                # Empty: in tuple2 = self.right_plan.get(False), when the queue is empty.
                # logging.info("Eddy {} Operator {} right_plan queue is empty".format(self.eddy ,self.id_operator))
                #self.probing.value = 0
                pass
            except TypeError:
                # TypeError: in resource = resource + tuple[var], when the tuple is "EOF".
                pass
            except IOError:
                # IOError: when a tuple is received, but the alarm is fired.
                pass

            if tuple1 and tuple2:
                if tuple1.data == "EOF" and tuple2.data == "EOF":
                    break

        # Perform the last probes.
        self.probing.value = 1
        self.stage3()
        self.probing.value = 0

    def stage1(self, tuple, tuple_rjttable, other_rjttable, vars):
        # Stage 1: While one of the sources is sending data.

        # Get the value(s) of the operator variable(s) in the tuple.
        resource = ''
        if tuple.data != "EOF":
            for var in self.vars:
                try:
                    resource = resource + str(tuple.data[var])
                    # print("{}: {}".format(self.id_operator, str(tuple1.data)))
                except Exception as e:
                    raise e
        else:
            resource = "EOF"

        # Probe the tuple against its RJT table.
        probe_ts = self.probe(tuple, resource, tuple_rjttable)

        # Create the records.
        record = Record(tuple, probe_ts, time(), float("inf"))

        # Insert the record in the corresponding RJT table.
        if resource in other_rjttable:
            other_rjttable.get(resource).updateRecords(record)
            other_rjttable.get(resource).setRJTProbeTS(probe_ts)
        else:
            tail = RJTTail(record, probe_ts)
            other_rjttable[resource] = tail

    def stage3(self):
        # Stage 3: When both sources sent all the data.
        # This is the optional: Produce tuples that haven't matched already.
        for tuple in self.bag:

            #print "From Bag: {}".format(tuple.data)
            res_right = {}
            for var in self.vars_right:
                res_right.update({var: ''})
            res = res_right
            res.update(tuple.data)

            ready = tuple.ready
            done =  tuple.done | pow(2, self.id_operator)
            sources = list(set(tuple.sources))

            # Create tuple.
            res_tuple = Tuple(res, ready, done, sources, self.id_operator)
            self.qresults[self.eddy].put(res_tuple)

        # Put EOF in queue and exit.
        #self.qresults[self.eddy].put(self.eof)

    def probe(self, tuple, resource, rjttable):

        probe_ts = time()

        # If the resource is in the table, produce results.
        if resource in rjttable:
            rjttable.get(resource).setRJTProbeTS(probe_ts)
            list_records = rjttable[resource].records

            # Delete tuple from bag.
            try:
                self.bag.remove(tuple)
            except ValueError:
                pass

            for record in list_records:
                #print record.tuple.data
                if resource != "EOF":
                    # Merge solution mappings.
                    data = {}
                    data.update(record.tuple.data)
                    data.update(tuple.data)
                else:
                    data = "EOF"

                # Update ready and done vectors.
                ready = record.tuple.ready | tuple.ready
                done = record.tuple.done | tuple.done | pow(2, self.id_operator)
                sources = list(set(record.tuple.sources) | set(tuple.sources))

                # Create tuple.
                res = Tuple(data, ready, done, sources, self.id_operator)

                # Send tuple to eddy operators.
                self.qresults[self.eddy].put(res)

                # Delete tuple from bag.
                try:
                    self.bag.remove(record.tuple)
                except ValueError:
                    pass

        return probe_ts