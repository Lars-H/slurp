from multiprocessing import Process, Queue
from  nlde.engine.contact_source import contact_source, contact_source_bindings
from nlde.operators.operatorstructures import Tuple
from nlde.query import TriplePattern, Argument
from  time import time

class DependentOperator(object):
    """
    Implements a plan leaf that is resolved by a dependent physical operator.

    The execute() method reads tuples from the input queue and
    place them in the output queue.
    """

    def __init__(self, sources, server, query, sources_desc, variables=None, res=0):

        if variables is None:
            variables = []

        self.server = server

        if isinstance(sources, int):
            self.sources =  {sources: set([str(var)  for var in variables])}
        else:
            self.sources = sources

        self.sources_desc = sources_desc
        self.source_id = sources
        self.server = server
        self.query = query
        self.vars = set(variables)
        self.join_vars = set(variables)
        self.total_res = res
        self.height = 0
        self.p = None
        self.cost = None

    def __str__(self):
        return "Dependent: {} ({} @ {})".format(self.query, self.cardinality, ",".join("({}: {})".format(source,
                                                                                                       value) for
                                                                                       source,
                                                                                       value in
                                                                                       self.query.sources.items() ))

    @property
    def variables_dict(self):
        return self.query.variables_dict

    @property
    def cardinality(self):
        return self.query.cardinality


    @property
    def selectivity(self):
        return self.query.selectivity

    def compute_cost(self, cost_model):
        cost_function = cost_model[type(self)]
        self.cost = cost_function(self)
        return self.cost

    def execute(self, variables, instances, outputqueue ,ldf_server=None, p_list=None):
        #self.q = Queue()

        # Make instances a list, if not yet
        if not isinstance(instances, list):
            instances = [instances]

        # If pre-selection of ldf servers exists
        if ldf_server:
            ldf_servers = [ldf_server]
        else:
            ldf_servers = self.query.sources

        # Create process to contact sources.
        aux_queue = Queue()
        #self.p = Process(target=contact_source_bindings, args=(ldf_servers, self.query, aux_queue, instances,
        # list(variables)))

        #print ldf_servers, self.query, self.query.sources, instances, variables
        contact_source_bindings(ldf_servers, self.query, aux_queue, instances, list(variables))

        #self.p.start()
        sources = self.sources.keys()

        if p_list:
            p_list.put(self.p.pid)

        # Ready and done vectors.
        ready = self.sources_desc[self.sources.keys()[0]]
        done = 0

        # Get answers from the sources.
        data = aux_queue.get(True)
        while data != "EOF":
            # TODO: Check why this is needed.
            #data.update(inst)

            # Create tuple and put it in output queue.
            outputqueue.put(Tuple(data, ready, done, sources))

            # Get next answer.
            data = aux_queue.get(True)

        # Close the queue
        aux_queue.close()
        #self.p.terminate()
        request_cnt = data.get("requests")
        outputqueue.put(Tuple("EOF", ready, done, sources, requests={self.source_id: request_cnt}))


    def execute_old(self, variables, instances, outputqueue ,p_list=None):
        self.q = Queue()
        # Copy the query array and obtain variables.
        query = [self.query.subject.value, self.query.predicate.value, self.query.object.value]
        variables = list(variables)

        # Instantiate variables in the query.
        inst = {}
        for i in variables:
            inst.update({i: instances[i]})
            #inst_aux = str(instances[i]).replace(" ", "%%%")
            # Remove the %%% replacement as it does not work with the current LDF Server implementation
            inst_aux = str(instances[i])
            for j in (0, 1, 2):
                if query[j] == "?" + i:
                    query[j] = inst_aux

        tp = TriplePattern(Argument(query[0]), Argument(query[1]), Argument(query[2]))
        tp.sources = self.query.sources

        # We need to handle the case that all variables are instatiated
        vars = None
        if tp.variable_position == 0:
            vars = self.query.variables_dict

        # Create process to contact sources.
        aux_queue = Queue()
        self.p = Process(target=contact_source, args=(self.query.sources, tp, aux_queue, vars))

        self.p.start()
        sources = self.sources.keys()

        if p_list:
            p_list.put(self.p.pid)

        # Ready and done vectors.
        ready = self.sources_desc[self.sources.keys()[0]]
        done = 0

        # Get answers from the sources.
        data = aux_queue.get(True)
        while data != "EOF":
            # TODO: Check why this is needed.
            data.update(inst)

            # Create tuple and put it in output queue.
            outputqueue.put(Tuple(data, ready, done, sources))

            # Get next answer.
            data = aux_queue.get(True)

        # Close the queue
        aux_queue.close()
        self.p.terminate()
        outputqueue.put(Tuple("EOF", ready, done, sources))