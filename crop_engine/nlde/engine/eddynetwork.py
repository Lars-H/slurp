"""
Created on Mar 23, 2015

@author: Maribel Acosta

Updated on Mar 10, 2020
@author: Lars Heling

"""
from eddyoperator import EddyOperator
from nlde.query.sparql_parser import parse
from crop.query_plan_optimizer.idp_optimizer import IDP_Optimizer
from crop.query_plan_optimizer.federated_optimizer import Federated_Optimizer
from crop.query_plan_optimizer.nlde_optimizer import nLDE_Optimizer
from crop.query_plan_optimizer.ldff_optimizer import LDFF_Optimizer
from nlde.operators.operatorstructures import Tuple
from nlde.policy.nopolicy import NoPolicy
from multiprocessing import Process, Queue, active_children
from time import time
import os, signal

import logging
logger = logging.getLogger("nlde_debug")


class EddyNetwork(object):
    
    def __init__(self, **kwargs):

        self.query = kwargs.get("query", None)
        self.policy = kwargs.get("policy", NoPolicy())
        self.sources = kwargs.get("sources", "")
        self.n_eddy = kwargs.get("n_eddy", 2)
        self.explain = kwargs.get("explain", False)
        self.eofs = 0

        self.independent_operators = []
        self.join_operators = []
        self.eddy_operators = []
        
        self.eddies_queues = []
        self.operators_input_queues = {}
        self.operators_left_queues = []
        self.operators_right_queues = []

        self.tree = None
        self.operators_desc = None
        self.sources_desc = None
        self.eofs_operators_desc = None
        self.output_queue = None

        self.optimizer = kwargs.get("optimizer")

        # Stats
        self.triple_pattern_cnt = -1 # TODO: Implement tp count in query

        self.p_list = Queue()

        self.plan = None
        if self.query is None:
            logger.debug("No query provided")
        else:
            self.plan = self.__get_query_plan()


    def __get_query_plan(self):

        # Parse SPARQL query.
        queryparsed = parse(self.query)
        self.triple_pattern_cnt = 1 #1 queryparsed.triple_pattern_count

        # Start Timer
        start = time()
        # Create Plan
        plan = self.optimizer.create_plan(queryparsed)

        # Time the execution
        self.optimization_time = time() - start

        logger.debug(plan)
        #return None
        return plan

    def execute_plan(self, outputqueue, plan):

        self.tree = plan.tree
        self.eofs_operators_desc = plan.operators_desc
        self.sources_desc = plan.sources_desc
        self.operators_desc = plan.operators_desc
        self.eofs = plan.independent_sources
        self.policy.initialize_priorities(plan.plan_order)

        # Create eddies queues.
        for i in range(0, self.n_eddy+1):
            self.eddies_queues.append(Queue())

        # Create operators queues (left_plan and right_plan).
        for op in plan.operators:
            self.operators_input_queues.update({op.id_operator: []})
            for i in range(0, op.independent_inputs):
                self.operators_input_queues[op.id_operator].append(Queue())

        for i in range(1, self.n_eddy+1):
            eddy = EddyOperator(i, self.policy, self.eddies_queues, self.operators_desc, self.operators_input_queues,
                                plan.operators_vars, outputqueue, plan.independent_sources, self.eofs_operators_desc,
                                plan.operators_sym, plan.operators)
            p = Process(target=eddy.execute)
            p.start()
            self.p_list.put(p.pid)

        self.tree.execute(self.operators_input_queues, self.eddies_queues, self.p_list, self.operators_desc)

    def execute(self, outputqueue):

        if not self.plan is None:
            # If there are actually any sources to be contacted in the plan
            if len(self.plan) > 0:
                self.execute_plan(outputqueue, self.plan)
            else:
                # Otherwise, just send EOF tuple
                eof_tuple = Tuple("EOF", None, None, None)

                # One "EOF" per eddy expecetd
                for _ in range(self.n_eddy):
                    outputqueue.put(eof_tuple)
        else:
            raise Exception("No Query Physical Plan to execute")


    def execute_standalone(self, plan):

        self.output_queue = Queue()
        self.plan = plan
        self.execute_plan(self.output_queue, plan)
        count = 0
        tuples_per_operator = {}
        requests_per_subexpression = {}
        # Handle the rest of the query answer.
        while count < self.n_eddy:
            try:
                ri = self.output_queue.get(True)
                if ri.data == "EOF":
                    tuples_per_operator.update(ri.tuples_produced)
                    requests_per_subexpression.update(ri.requests)
                    count = count + 1
                else:
                    yield ri
            except Exception as e:
                break

        self.plan.logical_plan_stats(tuples_per_operator)
        self.plan.execution_requests = sum(requests_per_subexpression.values())
        self.stop_execution()


    def stop_execution(self, sig=None, err=None):
        # Finalize Execution and kill all processes
        self.output_queue.close()
        while not self.p_list.empty():
            pid = self.p_list.get()
            try:
                os.kill(pid, signal.SIGKILL)
            except OSError as e:
                pass

        for p in active_children():
            try:
                p.terminate()
            except OSError as e:
                pass
        #import sys
        #sys.exit(1)