#!/usr/bin/env python

"""
Created on Mar 25, 2015

@author: Maribel Acosta

Updated on Mar, 2020

@author: Anonymous

"""
import argparse
import os
import signal
import sys
import logging
import json
from multiprocessing import active_children, Queue
from time import time

from nlde.engine.eddynetwork import EddyNetwork
from nlde.policy.nopolicy import NoPolicy

from crop.query_plan_optimizer import get_optimizer
from crop.query_plan_optimizer.idp_optimizer import IDP_Optimizer

def get_options():

    parser = argparse.ArgumentParser(description="CROP: An nLDE-based TPF Client"
                                                 " with a cost model-based robust query plan optimizer")

    # nLDE arguments.
    parser.add_argument("-s", "--sources",
                        help="List of URLs of the triple pattern fragment servers (required)", nargs="+", required=True)
    parser.add_argument("-f", "--queryfile",
                        help="file name of the SPARQL query (required, or -q)")
    parser.add_argument("-q", "--query",
                        help="SPARQL query (required, or -f)")
    parser.add_argument("-r", "--printres",
                        help="format of the output results",
                        choices=["y", "n", "f" ,"all"],
                        default="y")
    parser.add_argument("-e", "--eddies",
                        help="number of eddy processes to create",
                        type=int,
                        default=2)
    parser.add_argument("-t", "--timeout",
                        help="query execution timeout in seconds.",
                        type=int)
    parser.add_argument("-v", "--verbose",
                        help="print logging information",
                        choices=["INFO", "DEBUG", "ALL"])

    # CROP Specific Arguments

    #
    # Federated Parameters
    parser.add_argument("-j", "--pbj",
                        help="Polymorphic Bind Join (Optional, default=True)",
                        choices=["True", "False"],
                        default = "True")

    parser.add_argument("-i", "--decomposer",
                        help="Interface-aware decomposer (Optional, default=True)",
                        choices=["True", "False"],
                        default = "True")

    parser.add_argument("-p", "--prune_sources",
                        help="Apply pruning approach to sources (default=True)",
                        choices=["True", "False"],
                        default="True")

    # IDP Optimizer Parameters
    parser.add_argument("-o", "--optimizer",
                        help="Set the optimizer, default = IDP",
                        choices=["nlde", "idp", "ldff"],
                        default="ldff")
    parser.add_argument("-d", "--height_discount",
                        help="Discount for NLJ higher in the plan (Optional)",
                        type=float,
                        default=4.0)
    parser.add_argument("-c", "--cost_threshold",
                        help="Cost threshold for the query plan optimizer(Optional)",
                        type=float,
                        default=0.3)
    parser.add_argument("-u", "--robust_threshold",
                        help="Robustness threshold for the query plan optimizer(Optional)",
                        type=float,
                        default=0.05)
    parser.add_argument("-k", "--k",
                        help="Parameter k in IDP (Optional)",
                        type=int,
                        default=4)
    parser.add_argument("-a", "--adaptive_k",
                        help="Adaptive k (Optional)",
                        choices=["True", "False"])
    parser.add_argument("-b", "--top_t",
                        help="Top t plans to consider in IDP (Optional)",
                        type=int,
                        default=5)


    args = parser.parse_args()

    #print args

    # Handling mandatory arguments.
    err = False
    msg = []
    if not args.sources:
        err = True
        msg.append("error: no server specified. Use argument -s to specify the address of a server.")

    if not args.queryfile and not args.query and not args.json_plan:
        err = True
        msg.append("error: no query specified. Use argument -f, -q or -j to specify a query or a query plan.")

    if args.verbose == "INFO" or args.verbose == "ALL":
        logger = logging.getLogger("nlde_logger")
        logger.setLevel(logging.INFO)
        handler = logging.FileHandler('logs/nlde.log')
        handler.setLevel(logging.INFO)
        logger.addHandler(handler)

    if args.verbose == "DEBUG" or args.verbose == "ALL":
        if args.verbose != "ALL":
            logging.getLogger("nlde_logger").setLevel(logging.WARNING)

        logger = logging.getLogger("nlde_debug")
        logger.setLevel(logging.DEBUG)

    elif not args.verbose == "INFO":
        logger = logging.getLogger("nlde_logger")
        logger.setLevel(logging.WARNING)

    if err:
        parser.print_usage()
        print ("\n".join(msg))
        sys.exit(1)

    new_sources = []
    for source in args.sources:
        if not "@" in source:
            source = "tpf@{}".format(source)
        new_sources.append(source)
    args.sources = new_sources
    return args


class NLDE(object):


    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

        self.query_id = ""
        self.network = None
        self.p_list = None
        self.res = Queue()

        self.optimizer = get_optimizer(**kwargs)


        # Open query from file.
        if hasattr(self, "queryfile") and self.queryfile:
            self.query = open(self.queryfile).read()
            self.query_id = self.queryfile[self.queryfile.rfind("/") + 1:]
            self.query_dir = self.queryfile[:self.queryfile.rfind("/") + 1]


        # Set routing policy.
        # Set no routing policy
        self.policy = NoPolicy()

        # Set execution variables.
        self.init_time = None
        self.time_first = None
        self.time_total = None
        self.card = 0
        self.xerror = ""

        # Set execution timeout.
        if self.timeout:
            signal.signal(signal.SIGALRM, self.call_timeout)
            signal.alarm(self.timeout)


    def execute(self):
        logger = logging.getLogger("nlde_logger")

        if logger:
            logger.info('START %s', self.query_id)
            fhandler = logging.FileHandler('logs/{}.log'.format(self.query_id), 'w')
            fhandler.setLevel(logging.INFO)
            logger.addHandler(fhandler)

        # Initialization timestamp.
        self.init_time = time()
        self.time_last = 0

        # Create eddy network.
        if hasattr(self, "query") and self.query:
            network = EddyNetwork(query=self.query, policy=self.policy, sources=self.sources,
                                  n_eddy=self.eddies, optimizer=self.optimizer)

        else:
            sys.exit("Could not create Eddy Network")

        self.network = network

        self.p_list = network.p_list


        if self.printres == "y":
            self.print_solutions(network)
        elif self.printres == "all":
            self.print_all(network)
        elif self.printres == "f":
            self.solutions_to_file(network)
        else:
            self.print_basics(network)

        self.extended_summary(network)

    # Print only basic stats, but still iterate over results.
    def print_basics(self, network):

        network.execute(self.res)

        # Handle the first query answer.
        ri = self.res.get(True)
        self.time_first = time() - self.init_time
        count = 0
        if ri.data == "EOF":
            count = count + 1
        else:
            self.card = self.card + 1

        # Handle the rest of the query answer.
        while count < network.n_eddy:
            ri = self.res.get(True)
            if ri.data == "EOF":
                count = count + 1
            else:
                self.card = self.card + 1
                self.time_last = time() - self.init_time

        self.time_total = time() - self.init_time

    # Print only solution mappings.
    def print_solutions(self, network):

        network.execute(self.res)

        # Handle the first query answer.
        ri = self.res.get(True)
        self.time_first = time() - self.init_time
        count = 0
        if ri.data == "EOF":
            count = count + 1
        else:
            self.card = self.card + 1
            print (str(ri.data))

        # Handle the rest of the query answer.
        while count < network.n_eddy:
            ri = self.res.get(True)
            if ri.data == "EOF":
                count = count + 1
            else:
                self.time_last = time() - self.init_time
                self.card = self.card + 1
                print (str(ri.data))

        self.time_total = time() - self.init_time

    # Print all stats for each solution mapping.
    def print_all(self, network):

        network.execute(self.res)

        # Handle the first query answer.
        ri = self.res.get(True)
        self.time_first = time() - self.init_time
        count = 0
        if ri.data == "EOF":
            count = count + 1
        else:
            self.card = self.card + 1
            print (self.query_id + "\t" + str(ri.data) + "\t" + str(self.time_first) + "\t" + str(self.card))

        # Handle the rest of the query answer.
        while count < network.n_eddy:
            ri = self.res.get (True)
            if ri.data == "EOF":
                #print ri.ready, ri.done, ri.sources, ri.from_operator, ri.to_operator
                count = count + 1
            else:
                self.card = self.card + 1
                t = time() - self.init_time
                self.time_last = time() - self.init_time
                print (self.query_id + "\t" + str(ri.data) + "\t" + str(t) + "\t" + str(self.card))

        self.time_total = time() - self.init_time

    # Print all stats for each solution mapping.
    def solutions_to_file(self, network):

        results = []
        network.execute(self.res)

        # Handle the first query answer.
        ri = self.res.get(True)
        self.time_first = time() - self.init_time
        count = 0
        if ri.data == "EOF":
            count = count + 1
        else:
            self.card = self.card + 1
            results.append(ri.data)
            #print(ri.data)

        # Handle the rest of the query answer.
        while count < network.n_eddy:
            ri = self.res.get(True)
            if ri.data == "EOF":
                count = count + 1
            else:
                self.card = self.card + 1
                results.append(ri.data)
                self.time_last = time() - self.init_time
                #if len(ri.data.keys()) != 3:
                #print (len(ri.data.keys()))

        self.time_total = time() - self.init_time

        output_dir = "results"
        if not os.path.exists(output_dir):
            os.mkdir(output_dir)
        out_fn = "{}/{}_{}_results.json".format(output_dir,self.query_id.replace(".rq", ""), self.optimizer)
        with open(out_fn, "w") as result_file:
            json.dump(results, result_file)


    # Final stats of execution.
    def basic_summary(self):
        print (self.query_id + "\t" + str(self.time_first) + "\t" + str(self.time_total) + \
              "\t" + str(self.card) + "\t" + str(self.xerror))

    def experiment_summary(self, network, timeout=False):

        result_dct = {}
        t_total = self.time_total
        t_first = self.time_first
        opt_time = network.optimization_time
        est_card = network.plan.cardinality(network.cost_model)
        cost = network.plan.cost(network.cost_model)
        robustness = network.plan.average_cost(network.robust_model)
        bushy = network.plan.is_bushy

        if timeout:
            t_total = self.timeout

        idp_params = network.optimizer.params
        cost_model_params = network.cost_model.params
        params = [
            self.query_id, t_first, t_total, self.card, est_card, self.optimizer, opt_time, cost, robustness, bushy,
            network.triple_pattern_cnt
        ]
        params.extend(idp_params)
        params.extend(cost_model_params)
        request_count, elapsed_time, elapsed_sum = self.get_request_count()
        params.append(request_count)
        params.append(self.query_dir)
        print ("\t".join(str(param) for param in params))

    def extended_summary(self, network, timeout=False):

        result_dct = {
            "query": self.query_id,
            "runtime_s": self.time_total,
            "answers": self.card,
            "optimization_time_s": network.optimization_time,
            "triple_pattern_count": network.triple_pattern_cnt
        }
        result_dct.update(network.optimizer.params_dct)

        if isinstance(network.optimizer, IDP_Optimizer):
            cost_model_params = network.optimizer.cost_model.params_dct
            result_dct.update({
                "bc_cost_p_star" : network.plan.cost(network.optimizer.cost_model),
                "ac_cost_p_star" : network.plan.average_cost(network.optimizer.robust_model)
            })
            result_dct.update(cost_model_params)

        request_count, elapsed_time, elapsed_sum  = self.get_request_count()
        if request_count == 0:
            request_count = "NA"
            avg_elapsed = "NA"
            elapsed_sum = "NA"
        else:
            try:
                avg_elapsed = elapsed_time / elapsed_sum
            except ZeroDivisionError:
                avg_elapsed = "DivZero"
        result_dct['requests'] = request_count
        result_dct['average_request_time'] = avg_elapsed
        result_dct['first_result_elpased_s'] = self.time_first

        print result_dct

    # Timeout was fired.
    def call_timeout(self, sig, err):
        self.time_total = time() - self.init_time
        self.finalize()
        #self.experiment_summary(self.network, True)
        self.extended_summary(network=self.network, timeout=True)
        sys.exit(1)

    # Finalize execution: kill sub-processes.
    def finalize(self):

        self.res.close()

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

        logger = logging.getLogger("nlde_logger")
        if logger:
            logger.info('END %s', self.query_id)

    def get_request_count(self):
        from ast import literal_eval
        request = 0
        elapsed = 0
        interface2elapsed = {}
        all_elapsed = []
        logger = logging.getLogger("nlde_logger")
        if logger:
            with open('logs/{}.log'.format(self.query_id), "r") as infile:
                try:
                    for line in infile.readlines():
                        if "request" in line:
                            dct = literal_eval(line)
                            request += dct['requests']
                        if "elapsed" in line:
                            dct = literal_eval(line)
                            elapsed = float(dct['elapsed'])
                            all_elapsed.append(elapsed)
                            interface = dct['interface']
                            interface2elapsed[interface] = interface2elapsed.get(interface, 0) + elapsed
                    return request, sum(all_elapsed), len(all_elapsed)
                except Exception as e:
                    print e
                    return -1
        return -1


if __name__ == '__main__':

    options = get_options()
    nlde = NLDE(**vars(options))
    nlde.execute()
    nlde.finalize()
    sys.exit(0)