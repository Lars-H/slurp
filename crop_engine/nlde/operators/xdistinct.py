"""
Created on Mar 21, 2015

Physical operator that implements a DISTINCT.
This implementation is an extension of the Xdistinct operator
(see ANAPSID https://github.com/anapsid/anapsid).

The intermediate results are represented in a queue.

@author: Maribel Acosta
"""
from random import randint
from multiprocessing import Value#, Queue
from Queue import Empty


class Xdistinct(object):
    
    def __init__(self, id_operator, eddies):
        self.id_operator = id_operator
        self.left = None
        self.right = None
        self.qresults = None
        self.eddies = eddies
        self.eddy = 1 #randint(1, self.eddies)
        self.bag = {}
        self.probing = Value('i', 1)
        self.wait = True
        self.independent_inputs = 1
        self.produced_tuples = 0

    def __str__(self):
        return "XDistinct"

    def to_queue(self, res, source=None):

        if res.data == "EOF":
            res.tuples_produced.update({self.id_operator : self.produced_tuples})
        self.produced_tuples += 1
        self.qresults[self.eddy].put(res)

    # Executes the Xdistinct operator.
    def execute(self, inputs, out):
        # Initialize input and output queues.
        self.left = inputs[0]
        self.qresults = out

        # Get the tuples (solution mappings) from the input queue.
        while True:

            try:
                # Get next tuple (solution mapping).
                tuple1 = self.left.get(self.wait)

                if tuple1.data != "EOF":
                    str_tuple = frozenset(tuple1.data.items())
                else:
                    str_tuple = "EOF"
                    self.wait = False

                # Check distinct.
                distinct = self.bag.get(str_tuple, True)

                # Put tuple with solution mapping to output queue.
                if distinct:
                    tuple1.done = tuple1.done | pow(2, self.id_operator)
                    tuple1.from_operator = self.id_operator
                    self.to_queue(tuple1)
                    self.bag.update({str_tuple: False})

            except Empty:
                self.probing.value = 0