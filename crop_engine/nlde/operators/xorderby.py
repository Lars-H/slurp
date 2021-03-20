"""
Created on July 16, 2020

Physical operator that implements a ORDER BY.

The intermediate results are stored in queues and processed incrementally.

@author: Lars Heling
"""
from random import randint
from multiprocessing import Value  # , Queue
from Queue import Empty
from nlde.util.misc import extractValue

class Xorderby(object):

    def __init__(self, id_operator, eddies, args):
        self.id_operator = id_operator
        self.left = None
        self.right = None
        self.qresults = None
        self.eddies = eddies
        self.eddy = randint(1, self.eddies)
        self.vars = []
        self.probing = Value('i', 1)
        self.wait = True
        self.independent_inputs = 1
        self.args = args


    def __str__(self):
        return "Xorderby"

    # Executes the Order By.
    def execute(self, inputs, out):
        # Initialize input and output queues.
        self.left = inputs[0]
        # self.right_plan = Queue()
        self.qresults = out

        all_results = []
        results = []
        tuple_id = 0
        # Get the tuples (solution mappings) from the input queue.
        while True:

            try:
                # Get tuple (with solution mapping).
                self.probing.value = 1
                tuple = self.left.get(self.wait)

                # Perform projection in the solution mapping.
                if tuple.data != "EOF":
                    data = {}
                    data.update(tuple.data)
                    # print "tuple", tuple
                    for arg in self.args:
                        data.update({arg.name[1:]: extractValue(tuple.data[arg.name[1:]])[0]})
                    results.append(data)

                    data.update({'__id__': tuple_id})
                    tuple.data = data
                    all_results.append(tuple)
                    tuple_id = tuple_id + 1

                else:
                    self.wait = False
                    self.probing.value = 0
                    break
            except Empty:
                self.probing.value = 0

        # Sorting.
        self.args.reverse()
        # print "en order by ",self.args
        for arg in self.args:
            order_by = "lambda d: (d['" + arg.name[1:] + "'])"
            results = sorted(results, key=eval(order_by), reverse=arg.desc)

        # Add results to output queue.
        for data in results:
            res = all_results[data['__id__']]
            del res.data['__id__']
            res.done = res.done | pow(2, self.id_operator)
            self.qresults[self.eddy].put(res)


        # Put EOF in queue and exit.
        tuple.data = "EOF"
        tuple.done = tuple.done | pow(2, self.id_operator)
        self.qresults[self.eddy].put(tuple)
        self.probing.value = 0