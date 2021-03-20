"""
Created on Mar 21, 2015

Physical operator that implements a JOIN.
This implementation is an extension of the Xnjoin operator
(see ANAPSID https://github.com/anapsid/anapsid).

The intermediate results are stored in queues and processed incrementally.

@author: Maribel Acosta
"""
from multiprocessing import Queue, Value
from operatorstructures import Tuple, Record, RJTTail
from time import time
from random import randint

class Xnjoin(object):

    def __init__(self, id_operator, joinvars, eddies, eddy=None):
        self.left_table = dict()
        self.right_table = dict()
        self.id_operator = id_operator
        #self.vars = set(joinvars)
        self.vars = joinvars #set([str(var) for var in joinvars])
        self.eof = Tuple("EOF", 0, 0, set(), self.id_operator)
        self.eddies = eddies
        if eddy:
            self.eddy = eddy
        else:
            self.eddy = randint(1, self.eddies)
        self.empty_answers = []
        self.left = None
        self.right = None
        self.qresults = None
        self.sources = None
        self.probing = Value('i', 1)
        self.independent_inputs = 1
        self.produced_tuples = 0


    def to_dict(self):
        return {
            "type": str(self),
            "id_operator" : self.id_operator,
            "variables": list(self.vars),
            "eddies" : self.eddies,
            "eddy" : self.eddy
        }

    def __str__(self):
        return str("Xnjoin")

    @staticmethod
    def symmetric():
        return False

    def execute(self, inputs, out):

        # Executes the Xnjoin.
        self.left = inputs[0]
        self.right = inputs[1]
        self.qresults = out

        # Get tuples from queue.
        tuple1 = self.left.get(True)

        # Get the tuples from the queues.
        while not(tuple1.data == "EOF"):
            self.stage1(tuple1, self.left_table, self.right_table)
            tuple1 = self.left.get(True)

        # Add EOF to queue.
        #self.probing.value = 1
        tuple1.done = tuple1.done | pow(2, self.id_operator)
        tuple1.ready = self.right.sources_desc[self.right.sources.keys()[0]] | tuple1.ready
        tuple1.sources = set(tuple1.sources) | set([self.right.sources.keys()[0]])
        tuple1.from_operator = self.id_operator
        tuple1.tuples_produced.update({self.id_operator : self.produced_tuples})
        self.to_queue(tuple1)
        self.probing.value = 0

        # Finalize.
        self.stage3()


    def to_queue(self, res, source=None):
        self.produced_tuples += 1
        self.qresults[self.eddy].put(res)

    # Stage 1: While one of the sources is sending data.
    def stage1(self, tuple1, tuple_rjttable, other_rjttable):

        if tuple1.data != "EOF" and tuple1 != "EOF":

            # Get the value(s) of the operator variable(s) in the tuple.
            resource = ''
            for var in self.vars:
                resource = resource + str(tuple1.data[var])

            # Probe the tuple against its RJT table.
            probe_ts = self.probe(tuple1, resource, tuple_rjttable, other_rjttable)

            # Create the record.
            record = Record(tuple1, probe_ts, time(), float("inf"))

            # Insert the record in the other RJT table.
            # TODO: use RJTTail. Check ProbeTS
            if resource in other_rjttable:
                other_rjttable.get(resource).updateRecords(record)
                other_rjttable.get(resource).setRJTProbeTS(probe_ts)
            else:
                tail = RJTTail(record, float("inf"))
                other_rjttable[resource] = tail



    # Stage 2: When both sources become blocked.
    def stage2(self):
        pass

    # Stage 3: When both sources sent all the data.
    def stage3(self):
        return

    def probe(self, tuple1, resource, rjttable, other_rjttable):

        probe_ts = time()

        # If the resource is in table, produce results.
        if resource in rjttable.keys():

            rjttable.get(resource).setRJTProbeTS(probe_ts)
            list_records = rjttable[resource].records

            # For each match, produce the results (solution mappings).
            for record in list_records:
                res = {}

                if record.tuple.data == "EOF":
                    break

                # Merge solution mappings.
                res.update(record.tuple.data)
                res.update(tuple1.data)

                # Update ready and done vectors.
                ready = record.tuple.ready | tuple1.ready
                done = record.tuple.done | tuple1.done | pow(2, self.id_operator)
                sources = list(set(record.tuple.sources) | set(tuple1.sources))

                # Create solution mapping.
                res = Tuple(res, ready, done, sources, self.id_operator)

                # Send solution mapping to eddy operators.
                self.to_queue(res)
                #self.qresults[self.eddy].put(res)

        # If the resource is not in the table, contact the sources.
        else:

            # Extract domain and range of operator variables from the tuple.
            instances = {}
            for v in self.vars:
                instances.update({v : tuple1.data[v]})

            # Contact the sources.
            qright = Queue()
            self.right.execute(self.vars, instances, qright)

            # Get the tuples from right_plan queue.
            tuple2 = qright.get(True)
            self.sources = tuple2.sources

            # Empty result set.
            if (tuple2 == "EOF") or (tuple2.data == "EOF"):

                record = Record(tuple2, probe_ts, time(), float("inf"))
                tail = RJTTail(record, float("inf"))
                rjttable[resource] = tail

            # Non-empty result set.
            while (tuple2 != "EOF") and (tuple2.data != "EOF"):

                # Create solution mapping.
                data = {}
                data.update(tuple2.data)
                data.update(tuple1.data)

                #print("{}; {}".format(self.id_operator, data))
                # Update ready and done vectors of solution mapping.
                ready = tuple2.ready | tuple1.ready
                done = tuple2.done | tuple1.done | pow(2, self.id_operator)
                sources = list(set(tuple2.sources) | set(tuple1.sources))

                # Create tuple.
                res = Tuple(data, ready, done, sources, self.id_operator)

                # Send tuple to eddy operators.
                self.to_queue(res)
                #self.qresults[self.eddy].put(res)

                # Introduce the results of contacting the sources in the corresponding table.
                record = Record(tuple2, probe_ts, time(), float("inf"))
                if resource in rjttable.keys():
                    rjttable.get(resource).updateRecords(record)
                    rjttable.get(resource).setRJTProbeTS(probe_ts)
                else:
                    tail = RJTTail(record, float("inf"))
                    rjttable[resource] = tail

                # Get next solution.
                tuple2 = qright.get(True)

            # Close queue for this sources.
            qright.close()

        return probe_ts

