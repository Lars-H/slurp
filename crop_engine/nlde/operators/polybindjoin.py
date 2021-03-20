"""
Created on Sep 4, 2020

Physical operator that implements a JOIN.

The intermediate results are stored in queues and processed incrementally.

@author: Lars Heling
"""
from multiprocessing import Queue, Value, Process
from Queue import Empty
import math
from operatorstructures import Tuple, Record, RJTTail
from time import time
from random import randint
from nlde.util.misc import compatible_solutions
from nlde.operators.independent_operator import IndependentOperator

import logging
logger = logging.getLogger("nlde_debug")


class Poly_Bind_Join(object):

    def __init__(self, id_operator, joinvars, eddies, eddy=None, left_card=-1):
        self.left_table = dict()
        self.right_table = dict()
        self.id_operator = id_operator
        self.vars = joinvars
        self.eof = Tuple("EOF", 0, 0, set(), self.id_operator)
        self.eddies = eddies
        if eddy:
            self.eddy = eddy
        else:
            self.eddy = randint(1, self.eddies)
        self.left = None
        self.right = None
        self.qresults = None
        self.sources = None
        self.probing = Value('i', 1)
        self.independent_inputs = 1
        self.results_per_source = {}
        self.left_est_card = left_card

        self.produced_tuples = 0

        # Config
        self.__type2limit = {
            "tpf" : 1,
            "brtpf" : 30,
            "sparql" : 50
        }

    def __str__(self):
        return str("Poly Bind Join")

    @staticmethod
    def symmetric():
        return False

    def execute(self, inputs, out):

        # Executes the Xnjoin.
        self.left = inputs[0]
        self.right = inputs[1]
        self.qresults = out

        self.limits = {}
        self.reservoir = {}
        self.tables = {}

        # Stats for deciding swicth
        tuples_left_cnt = 0
        right_requests = int(math.ceil(self.right.query.cardinality / 100.0))
        switch_left_count = right_requests - self.left_est_card

        relevant_sources = self.right.query.sources.keys()
        for ldf_source in relevant_sources:
            self.limits[ldf_source] = self.__type2limit.get(ldf_source.split("@")[0], 1)
            self.reservoir[ldf_source] = list()
            self.tables[ldf_source] = {}
            self.results_per_source[ldf_source] = 0

        # Get tuples from queue.
        tuple1 = self.left.get(True)
        tuples_left_cnt +=1
        switch_mode = False
        #
        # Get the tuples from the queues.
        while not(tuple1.data == "EOF"):

            for ldf_server in [k for k, v in sorted(self.results_per_source.items(), key=lambda item: item[1],
                                                    reverse=True)]:
                self.stage1(tuple1, self.right, ldf_server)

            # Check whether to switch:
            if tuples_left_cnt > switch_left_count and tuples_left_cnt > self.left_est_card:
                switch_mode = True
                break

            tuple1 = self.left.get(True)
            tuples_left_cnt += 1

        if switch_mode:
            logger.debug("NLJ: Switching Mode to Hash Join")
            logger.debug("Left tuples expected : {}; received: {}; switch count: {}; right_plan request remaining: {}".
                         format(self.left_est_card, tuples_left_cnt, switch_left_count, right_requests))
            self.hash_join_mode()

        else:
            for ldf_server in relevant_sources:
                self.stage1(tuple1, self.right, ldf_server)

            # Add EOF to queue.
            #self.probing.value = 1
            tuple1.done = tuple1.done | pow(2, self.id_operator)
            tuple1.ready = self.right.sources_desc[self.right.sources.keys()[0]] | tuple1.ready
            tuple1.sources = set(tuple1.sources) | set([self.right.sources.keys()[0]])
            tuple1.from_operator = self.id_operator
            tuple1.tuples_produced.update({self.id_operator : self.produced_tuples})

            self.to_queue(tuple1)
            self.probing.value = 0


    # Stage 1: While one of the sources is sending data.
    def stage1(self, tuple1, right, ldf_server):

        if tuple1.data != "EOF" and tuple1 != "EOF":

            # Check if the data is already in its sources tuple table
            rtuple = self.probe_table_of_source(tuple1, right, ldf_server, self.tables[ldf_server])
            if rtuple:
                self.reservoir[ldf_server].append(rtuple)

            if len(self.reservoir[ldf_server]) >= self.limits[ldf_server]:
                self.probe_tuples_from_source(self.reservoir[ldf_server], right, ldf_server, self.tables[ldf_server])
                self.reservoir[ldf_server] = list()#set()

        elif len(self.reservoir[ldf_server]) > 0:
            resevoir = list()
            for res_tuple in self.reservoir[ldf_server]:
                rtuple = self.probe_table_of_source(res_tuple, right, ldf_server, self.tables[ldf_server])
                if rtuple:
                    resevoir.append(rtuple)
            self.probe_tuples_from_source(resevoir, right, ldf_server, self.tables[ldf_server])

    def to_queue(self, res, source):
        self.produced_tuples += 1
        self.results_per_source[source] = self.results_per_source.get(source, 0) + 1
        self.qresults[self.eddy].put(res)


    def probe_table_of_source(self, rtuple, right, ldf_server, tuple_rjttable):

        # Get the value(s) of the operator variable(s) in the tuple.
        resource = ''
        for var in self.vars:
            resource = resource + str(rtuple.data[var])

        probe_ts = time()

        # If the resource is in table, produce results.
        if resource in tuple_rjttable.keys():


            tuple_rjttable.get(resource).setRJTProbeTS(probe_ts)
            list_records = tuple_rjttable[resource].records

            # For each match, produce the results (solution mappings).
            for record in list_records:
                res = {}

                if record.tuple.data == "EOF":
                    break

                # Merge solution mappings.
                res.update(record.tuple.data)
                res.update(rtuple.data)

                # Update ready and done vectors.
                ready = record.tuple.ready | rtuple.ready
                done = record.tuple.done | rtuple.done | pow(2, self.id_operator)
                sources = list(set(record.tuple.sources) | set(rtuple.sources))

                # Create solution mapping.
                res = Tuple(res, ready, done, sources, self.id_operator)

                # Send solution mapping to eddy operators.
                self.to_queue(res, ldf_server)
            return None
        else:
            return rtuple

    def probe_tuples_from_source(self, tuple_list, right, ldf_server, tuple_rjttable):

        probe_ts = time()
        if len(tuple_list) > 0:
            instances = []
            for rtuple in tuple_list:
                instance = {}
                for v in self.vars:
                    instance.update({v: rtuple.data[v]})
                instances.append(instance)

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
                    self.to_queue(res, ldf_server)

                    if resource in tuple_rjttable.keys() and not rtuple_added:
                        tuple_rjttable.get(resource).updateRecords(record)
                        tuple_rjttable.get(resource).setRJTProbeTS(probe_ts)
                    else:
                        tail = RJTTail(record, float("inf"))
                        tuple_rjttable[resource] = tail
                        rtuple_added = True



                # Get next solution.
                tuple2 = qright.get(True)

            # Close queue for this sources.
            qright.close()



    def hash_join_mode(self):

        self.p_list = []
        # Create Independent Operator for the right_plan side
        self.right_operator = IndependentOperator(self.right.sources.keys()[0], self.right.server, self.right.query,
                                                  self.right.sources_desc, eofs_desc=self.right.sources_desc)

        # Create Queue for it
        self.right = Queue()
        p2 = Process(target=self.right_operator.execute,
                     args=(None, self.right, None, self.p_list,))

        # Execute Operator
        p2.start()
        self.p_list.append(p2.pid)

        while True:

            self.probing.value = 1
            # Try to get and process tuple from left_plan queue.
            try:
                tuple1 = self.left.get(False)
                self.stage1_hash_join(tuple1, self.left_table, self.right_table)

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
                self.stage1_hash_join(tuple2, self.right_table, self.left_table)

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


    # Stage 1: While one of the sources is sending data.
    def stage1_hash_join(self, tuple1, tuple_rjttable, other_rjttable):

        # Get the value(s) of the operator variable(s) in the tuple.
        resource = ''
        if tuple1.data != "EOF":
            for var in self.vars:
                try:
                    resource = resource + str(tuple1.data[var])
                except Exception as e:
                    raise e
        else:
            resource = "EOF"

        # Probe the tuple against its RJT table.
        probe_ts = self.probe_hash_table(tuple1, resource, tuple_rjttable)

        # Create the records.
        record = Record(tuple1, probe_ts, time(), float("inf"))

        # Insert the record in the corresponding RJT table.
        if resource in other_rjttable:
            other_rjttable.get(resource).updateRecords(record)
            other_rjttable.get(resource).setRJTProbeTS(probe_ts)
        else:
            tail = RJTTail(record, probe_ts)
            other_rjttable[resource] = tail


    def probe_hash_table(self, tuple1, resource, rjttable):

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

                # Send tuple to eddy operators.
                self.qresults[self.eddy].put(res)

        return probe_ts