"""
Created on Jan 15, 2020

Physical operator that implements a UNION.


@author: Lars Heling
"""
from random import randint
from multiprocessing import Value
from Queue import Empty

import logging
logging.basicConfig(level=logging.INFO)

class Xunion(object):

    def __init__(self, id_operator, eddies, inputs=1):
        self.id_operator = id_operator
        self.left = None
        self.right = None
        self.qresults = None
        self.eddies = eddies
        self.eddy = randint(1, self.eddies)
        self.probing = Value('i', 1)
        self.wait = True
        self.independent_inputs = 1
        self.inputs = inputs

    # Executes the Xunion operator.
    def execute(self, inputs, out):
        # Initialize input and output queues.
        self.left = inputs[0]
        self.qresults = out

        # Get the tuples (solution mappings) from the input queue.
        while True:

            try:
                # Get tuple (with solution mapping).
                self.probing.value = 1
                tuple1 = self.left.get(self.wait)

                # Perform projection in the solution mapping.
                if tuple1.data != "EOF":
                    # Create solution mapping with the specified domain in self.vars.
                    tuple1.done = tuple1.done | pow (2, self.id_operator)
                    self.qresults[self.eddy].put(tuple1)
                else:
                    self.wait = False

                if tuple1.data == "EOF":
                    # We need to only send the EOF once
                    # So for all but the last "EOF" we will not put it on the result queue
                    if self.inputs > 1:
                        self.inputs = self.inputs - 1
                    else:
                        tuple1.done = tuple1.done | pow(2, self.id_operator)
                        self.qresults[self.eddy].put(tuple1)

            except Empty:
                self.probing.value = 0
