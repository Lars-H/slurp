"""
Created on Sep 1, 2020

Physical operator that implements a JOIN.

The intermediate results are stored in queues and processed incrementally.

@author: Lars Heling
"""

from multiprocessing import Value, Queue
from Queue import Empty
from operatorstructures import Tuple, Record, RJTTail
from time import time
from random import randint
import math
import os, signal

import datetime
import logging
logger = logging.getLogger("nlde_debug")
# For Logging the additional requests
request_logger = logging.getLogger("nlde_logger")

from nlde.util.misc import compatible_solutions
from nlde.operators.dependent_operator import DependentOperator


class Poly_Fjoin(object):

    def __init__(self, id_operator, variables, eddies, left_leaf, right_leaf, eddy=None):
        self.left_table = dict()
        self.right_table = dict()
        self.id_operator = id_operator
        self.vars = variables
        self.eof = Tuple("EOF", 0, 0, set(), self.id_operator)
        self.eddies = eddies

        self.eddy = randint(1, self.eddies)
        self.left = None
        self.right = None
        self.qresults = None
        self.probing = Value('i', 1)
        self.independent_inputs = 2

        self.left_leaf = left_leaf
        self.right_leaf = right_leaf

        # Poly Specific Properties
        self.__right_pid = None
        self.tuples_to_probe = []
        self.produced_tuples_list = []
        self.produced_tuples = 0

    def __str__(self):
        return str("Poly FJoin")

    @property
    def right_pid(self):
        return self.__right_pid

    @right_pid.setter
    def right_pid(self, pid):
        self.__right_pid = pid

    @staticmethod
    def symmetric():
        return True

    def abort_right_process(self):
        try:
            pid = self.right_pid
            os.kill(pid, signal.SIGKILL)
            return True
        except:
            return False

    def to_queue(self, res, source=None):

        self.produced_tuples += 1
        self.produced_tuples_list.append(res)
        # Send tuple to eddy operators.
        self.qresults[self.eddy].put(res)

    def execute(self, inputs, out):

        # Initialize input and output queues.
        self.left = inputs[0]
        self.right = inputs[1]
        self.qresults = out

        left_count = 0
        right_count = 0

        # Get the tuples from the input queues.
        while True:

            # Try to get and process tuple from left_plan queue.
            self.probing.value = 1
            try:
                tuple1 = self.left.get(False)
                self.stage1(tuple1, self.left_table, self.right_table)


                left_count += 1
                if tuple1.data == "EOF":
                    # TODO: Compute remaining requests based on the type of source (tpf vs. sparql)
                    right_requests_performed = right_count / 100.0
                    right_requests_remaining = math.ceil(float(self.right_leaf.total_res - right_count)) / 100
                    logger.debug(
                        "Left Received: {}; Expected: {}; Right Requests Performed: {}; Right Request Remaining: {"
                        "}".format(
                            left_count,
                            self.left_leaf.total_res,
                            right_requests_performed,
                            right_requests_remaining))
                    if 1.0 * left_count < right_requests_remaining:
                        # Clear remaining tuples in Queue
                        self.clear_queue(self.right)
                        # Switch Operation Mode to NLJ
                        self.tuples_to_probe = self.right_table
                        self.abort_right_process()

                        # Log Message counting requests
                        mgs = {
                            "requests": int(math.ceil(right_requests_performed)),
                            "timestamp": str(datetime.datetime.now()),
                            "source" : "Poly FJoin"
                        }
                        request_logger.info(str(mgs))

                        break
            except Empty:
                # Empty: in tuple1 = self.left_plan.get(False), when the queue is empty.
                self.probing.value = 0
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
                self.stage1(tuple2, self.right_table, self.left_table)

                right_count += 1
            except Empty:
                # Empty: in tuple2 = self.right_plan.get(False), when the queue is empty.
                self.probing.value = 0
                pass
            except TypeError:
                # TypeError: in resource = resource + tuple[var], when the tuple is "EOF".
                pass
            except IOError:
                # IOError: when a tuple is received, but the alarm is fired.
                pass


        if len(self.tuples_to_probe) > 0:
            logger.debug("Switched to NLJ Mode")
            # Create DP
            self.right_operator = DependentOperator(self.right_leaf.sources, self.right_leaf.server,
                                                  self.right_leaf.query,
                                           self.right_leaf.sources_desc)
            # Create new table
            self.probe_tuples(self.tuples_to_probe)
        else:
            self.probing.value = 0

    # Stage 1: While one of the sources is sending data.
    def stage1(self, tuple1, tuple_rjttable, other_rjttable):

        # Get the value(s) of the operator variable(s) in the tuple.
        resource = ''
        if tuple1.data != "EOF":
            for var in self.vars:
                try:
                    resource = resource + str(tuple1.data[var])
                    # print("{}: {}".format(self.id_operator, str(tuple1.data)))
                except Exception as e:
                    raise e
        else:
            resource = "EOF"

        # Probe the tuple against its RJT table.
        probe_ts = self.probe(tuple1, resource, tuple_rjttable)

        # Create the records.
        record = Record(tuple1, probe_ts, time(), float("inf"))

        # Insert the record in the corresponding RJT table.
        if resource in other_rjttable:
            other_rjttable.get(resource).updateRecords(record)
            other_rjttable.get(resource).setRJTProbeTS(probe_ts)
        else:
            tail = RJTTail(record, probe_ts)
            other_rjttable[resource] = tail

    def probe(self, tuple1, resource, rjttable):

        # Probe a tuple against its corresponding table.
        probe_ts = time()

        # If the resource is in the table, produce results.
        if resource in rjttable:
            rjttable.get(resource).setRJTProbeTS(probe_ts)
            list_records = rjttable[resource].records

            # For each matching solution mapping, generate an answer.
            for record in list_records:
                if resource != "EOF":
                    # Merge solution mappings.
                    data = {}
                    data.update(record.tuple.data)
                    data.update(tuple1.data)
                else:
                    data = "EOF"

                # Update ready and done vectors.
                ready = record.tuple.ready | tuple1.ready
                done = record.tuple.done | tuple1.done | pow(2, self.id_operator)
                sources = list(set(record.tuple.sources) | set(tuple1.sources))

                # Create tuple.
                res = Tuple(data, ready, done, sources, self.id_operator)

                # print(ready, done)
                #self.produced_tuples_list.append(res)
                # Send tuple to eddy operators.
                #self.qresults[self.eddy].put(res)
                self.to_queue(res)

        return probe_ts

    def probe_tuples(self, tuples):

        # TODO: Update to handle different binding capabilities (brTPF, SPARQL)
        right_tables = {}
        relevant_sources = self.right_operator.query.sources.keys()
        for ldf_source in relevant_sources:
            right_tables[ldf_source] = {}

        for key, tail in tuples.items():
            for record in tail.records:
                for ldf_server, table in right_tables.items():
                    self.probe_tuple([record.tuple], self.right_operator, ldf_server, table)

        # Put EOF in Queue
        tuple = record.tuple
        tuple.data = "EOF"
        tuple.done = tuple.done | pow(2, self.id_operator)
        tuple.ready = self.right_operator.sources_desc[self.right_operator.sources.keys()[0]] | tuple.ready
        tuple.sources = set(tuple.sources) | set([self.right_operator.sources.keys()[0]])
        tuple.from_operator = self.id_operator
        tuple.tuples_produced.update({self.id_operator : self.produced_tuples})

        self.to_queue(tuple)

        self.clear_queue(self.right)
        self.clear_queue(self.left)

        self.probing.value = 0


    def probe_tuple(self, tuple_list, right, ldf_server, tuple_rjttable):

        probe_ts = time()
        if len(tuple_list) > 0:
            instances = []
            for rtuple in tuple_list:
                if rtuple.data != "EOF":
                    instance = {}
                    for v in self.vars:
                        instance.update({v: rtuple.data[v]})
                    instances.append(instance)

            if len(instances) > 0:

                # Contact the sources.
                qright = Queue()
                right.execute(self.vars, instances, qright, ldf_server=ldf_server)

                # Get the tuples from right_plan queue.
                tuple2 = qright.get(True)
                self.sources = tuple2.sources

                # Empty result set.
                if (tuple2 == "EOF") or (tuple2.data == "EOF"):

                    # For all tested tuples add the tail to the records
                    for tested_tuple in tuple_list:
                        resource = ''
                        for var in self.vars:
                            resource = resource + str(tested_tuple.data[var])
                        record = Record(tuple2, probe_ts, time(), float("inf"))
                        tail = RJTTail(record, float("inf"))
                        tuple_rjttable[resource] = tail

                # Non-empty result set.
                while (tuple2 != "EOF") and (tuple2.data != "EOF"):

                    rtuple_added = False
                    for rtuple in tuple_list:

                        if not compatible_solutions(rtuple.data, tuple2.data):
                            continue

                        # Create solution mapping.
                        data = {}
                        data.update(tuple2.data)
                        data.update(rtuple.data)
                        # Update ready and done vectors of solution mapping.
                        ready = tuple2.ready | rtuple.ready
                        done = tuple2.done | rtuple.done | pow(2, self.id_operator)
                        sources = list(set(tuple2.sources) | set(rtuple.sources))

                        # Create tuple.
                        res = Tuple(data, ready, done, sources, self.id_operator)

                        # Introduce the results of contacting the sources in the corresponding table.
                        record = Record(tuple2, probe_ts, time(), float("inf"))
                        resource = ''
                        for var in self.vars:
                            resource = resource + str(rtuple.data[var])

                        # Send tuple to eddy operators.
                        # Send it, if it has not been produced before
                        # TODO: Is this always correct?
                        # What if there are several identical mappings for the same variable from the left_plan side (We
                        # would need to keep track of the triple producing the tuple or remove it from the table)
                        if not res in self.produced_tuples_list:
                            self.to_queue(res)
                            #self.qresults[self.eddy].put(res)

                        if resource in tuple_rjttable.keys() and not rtuple_added:
                            tuple_rjttable.get(resource).updateRecords(record)
                            tuple_rjttable.get(resource).setRJTProbeTS(probe_ts)
                        else:
                            tail = RJTTail(record, float("inf"))
                            tuple_rjttable[resource] = tail
                            rtuple_added = True

                    # Get next solution.
                    tuple2 = qright.get(True)

                qright.close()


    def clear_queue(self, q):

        while not q.empty():
            try:
                q.get(False)
            except Empty:
                continue