'''

Created on Jul 14, 2020
Implements the Xnoptional operator.
The intermediate results are represented in a queue.
@author: Lars Heling

'''
from multiprocessing import Queue, Value
from time import time
from operatorstructures import Record, RJTTail, Tuple
from random import randint

class Xnoptional(object):

    def __init__(self, id_operator, vars_left, vars_right, eddies, eddy=None):
        self.left_table = dict()
        self.right_table = dict()
        self.id_operator = id_operator
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

        self.vars_left   = set(vars_left)
        self.vars_right  = set(vars_right)
        self.vars = self.vars_left.intersection(self.vars_right)

    def __str__(self):
        return str("Xnoptional")

    @staticmethod
    def symmetric():
        return False

    def execute(self, inputs, out):

        # Executes the Xnoptional.
        self.left = inputs[0]
        self.right = inputs[1]
        self.qresults = out

        # Get tuples from queue.
        tuple = self.left.get(True)

        # Get the tuples from the queues.
        while (not(tuple.data == "EOF")):
            self.stage1(tuple, self.left_table, self.right_table)
            tuple = self.left.get(True)

        tuple.done = tuple.done | pow(2, self.id_operator)
        tuple.ready = self.right.sources_desc[self.right.sources.keys()[0]] | tuple.ready
        tuple.sources = set(tuple.sources) | set([self.right.sources.keys()[0]])
        tuple.from_operator = self.id_operator
        tuple.data = "EOF"
        self.qresults[self.eddy].put(tuple)
        #self.qresults[self.eddy].put(self.eof)
        self.probing.value = 0

        # Perform the last probes.
        self.stage3()


    def stage1(self, tuple, tuple_rjttable, other_rjttable):
        # Stage 1: While one of the sources is sending data.

        # Get the resource associated to the tuples.
        if tuple.data != "EOF" and tuple != "EOF":
            resource = ''
            for var in self.vars:
                resource = resource + str(tuple.data[var])

            # Probe the tuple against its RJT table.
            probeTS = self.probe(tuple, resource, tuple_rjttable, other_rjttable)

            # Create the records.
            record = Record(tuple, probeTS, time(), float("inf"))

            # Insert the record in the other RJT table.
            # TODO: use RJTTail. Check ProbeTS
            if resource in other_rjttable:
                other_rjttable.get(resource).updateRecords(record)
                other_rjttable.get(resource).setRJTProbeTS(probeTS)
                #other_rjttable.get(resource).append(record)
            else:
                tail = RJTTail(record, float("inf"))
                other_rjttable[resource] = tail
                #other_rjttable[resource] = [record]

    def stage2(self):
        # Stage 2: When both sources become blocked.
        pass

    def stage3(self):
        # Stage 3: When both sources sent all the data.

        # Put EOF in queue and exit.
        #self.qresults.put("EOF")
        return

    def probe(self, tuple, resource, rjttable, other_rjttable):
        probeTS = time()

        # If the resource is in table, produce results.
        if resource in rjttable:
            rjttable.get(resource).setRJTProbeTS(probeTS)
            list_records = rjttable[resource].records
            #list_records = rjttable[resource]

            # For each match, produce the results (solution mappings).
            for record in list_records:
                res = {}

                if record.tuple.data == "EOF":
                    break

                # Merge solution mappings.
                res.update(record.tuple.data)
                res.update(tuple.data)

                # Update ready and done vectors.
                ready = record.tuple.ready | tuple.ready
                done = record.tuple.done | tuple.done | pow(2, self.id_operator)
                sources = list(set(record.tuple.sources) | set(tuple.sources))

                # Create solution mapping.
                res = Tuple(res, ready, done, sources, self.id_operator)

                # Send solution mapping to eddy operators.
                self.qresults[self.eddy].put(res)

        # If not, contact the source.
        else:
            instances = {}
            for v in self.vars:
                instances.update({v: tuple.data[v]})

            # Contact the source.
            qright = Queue()
            self.right.execute(self.vars, instances, qright)


            # Get the tuples from right_plan queue.
            rtuple = qright.get(True)
            self.sources = rtuple.sources

            if (not(rtuple.data == "EOF")):

                while (not(rtuple.data == "EOF")):
                    # Create solution mapping.
                    data = {}
                    data.update(rtuple.data)
                    data.update(tuple.data)

                    # print("{}; {}".format(self.id_operator, data))
                    # Update ready and done vectors of solution mapping.
                    ready = rtuple.ready | tuple.ready
                    done = rtuple.done | tuple.done | pow(2, self.id_operator)
                    sources = list(set(rtuple.sources) | set(tuple.sources))

                    # Create tuple.
                    res = Tuple(data, ready, done, sources, self.id_operator)

                    # Send tuple to eddy operators.
                    self.qresults[self.eddy].put(res)

                    # Create and insert the record in the left_plan RJT table.
                    record = Record(rtuple, probeTS, time(), float("inf"))
                    if resource in rjttable:
                        other_rjttable.get(resource).updateRecords(record)
                        other_rjttable.get(resource).setRJTProbeTS(probeTS)
                    else:
                        tail = RJTTail(record, float("inf"))
                        other_rjttable[resource] = tail

                    rtuple = qright.get(True)

            else:
                # Build the empty tuple.
                rtuple = {}
                for att in self.right.vars:
                    rtuple.update({att:''})

                # Produce the answer,
                rtuple.update(tuple.data)
                # Create tuple.
                sources = list(set(tuple.sources))
                done = tuple.done | pow(2, self.id_operator)
                ready = tuple.ready
                res = Tuple(rtuple, ready, done, sources, self.id_operator)
                self.qresults[self.eddy].put(res)

        return probeTS