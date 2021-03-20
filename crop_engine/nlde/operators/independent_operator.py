from multiprocessing import Process, Queue
from nlde.engine.contact_source import contact_source
from nlde.operators.operatorstructures import Tuple, EOF


class IndependentOperator(object):
    """
    Implements a plan leaf that can be resolved asynchronously.

    The execute() method reads tuples from the input queue and
    place them in the output queue.
    """

    def __init__(self, sources, server, query, sources_desc, variables=None, eddies=2, eofs_desc={}, res=-1, **kwargs):
        if variables is None:
            variables = []

        self.source_id = sources
        self.server = server
        self.sources = {sources: variables}
        self.server = server
        self.query = query
        self.sources_desc = sources_desc #{int(key): int(value) for key, value in sources_desc.items()}

        # False sources desc
        self.f_sources_desc = {int(key): int(value) for key, value in sources_desc.items()}
        self.vars = set([str(var) for var in variables])
        self.join_vars = set([str(var) for var in variables])
        self.total_res = res
        self.height = 0
        self.eddies = eddies
        self.eofs_desc = eofs_desc
        self.p = None
        self.cost = None
        self.sparql_limit = kwargs.get("sparql_limit", 500)

    def __str__(self):
        return "Independent: {} ({} @ {})".format(self.query, self.cardinality, ",".join("({}: {})".format(source,
                                                                                                         value) for
                                                                                       source,
                                                                                       value in
                                                                                       self.query.sources.items()))

    def aux(self, _):
        return " Independent: " + str(self.query)

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

    def execute(self, left, right, outputqueues, p_list=None):
        self.q = Queue()
        # Determine the output queue.
        if not left:
            outq = right
        else:
            outq = left

        # Contact sources.
        #self.p = Process(target=contact_source, args=(self.server, self.query, self.q,), kwargs={"sparql_limit"
        #                                                                                         : self.sparql_limit})
        #self.p.start()
        contact_source(self.server, self.query, self.q, sparql_limit=self.sparql_limit)

        #if p_list:
        #    p_list.put(self.p.pid)

        # Initialize signature of tuples.
        ready = self.sources_desc[self.sources.keys()[0]]

        done = 0
        sources = self.sources.keys()

        # Read answers from the sources.
        data = self.q.get(True)
        count = 0
        while data != "EOF":
            count = count + 1
            # Create tuple and put it in the output queue.
            outq.put(Tuple(data, ready, done, sources))
            # Read next answer.
            data = self.q.get(True)

        # Close queue.
        ready_eof = self.eofs_desc[self.sources.keys()[0]]

        request_cnt = data.get("requests")
        outq.put(Tuple("EOF", ready_eof, done, sources, requests={self.source_id : request_cnt}))
        outq.close()
        #self.p.terminate()