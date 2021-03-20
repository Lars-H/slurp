"""
Created on Mar 21, 2015

Physical operator that implements a PROJECTION.
This implementation is an extension of the Xproject operator
(see ANAPSID https://github.com/anapsid/anapsid).

The intermediate results are stored in queues and processed incrementally.

@author: Maribel Acosta
"""
from random import randint
from multiprocessing import Value#, Queue
from Queue import Empty


class Xproject(object):
    
    def __init__(self, id_operator, variables, eddies):
        self.id_operator = id_operator
        self.left = None
        self.right = None
        self.qresults = None
        self.eddies = eddies
        self.eddy = 1 #randint(1, self.eddies)
        self.vars = []
        self.probing = Value('i', 1)
        self.wait = True
        self.independent_inputs = 1

        for arg in variables:
            self.vars.append(arg.value[1:])


    def __str__(self):
        return "Xproject"


    # Executes the Xproject.
    def execute(self, inputs, out):
        # Initialize input and output queues.
        self.left = inputs[0]
        self.qresults = out
        # Get the tuples (solution mappings) from the input queue.
        while True:

            try:
                # Get tuple (with solution mapping).
                tuple1 = self.left.get(self.wait)
                # Perform projection in the solution mapping.
                if tuple1.data != "EOF":
                    # Create solution mapping with the specified domain in self.vars.
                    res = {}
                    for var in self.vars:
                        res.update({var: tuple1.data.get(var, 'null')})
                    tuple1.data = res
                else:
                    self.wait = False

                # Put tuple with solution mapping to output queue.
                tuple1.done = tuple1.done | pow (2, self.id_operator)
                self.qresults[self.eddy].put(tuple1)
            except Empty:
                self.probing.value = 0