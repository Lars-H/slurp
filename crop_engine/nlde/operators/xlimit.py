"""
Created on Jul 10, 2020

Physical operator that implements a LIMIT and OFFSET.

The intermediate results are stored in queues and processed incrementally.

@author: Lars Heling
"""
from random import randint
from multiprocessing import Value
from Queue import Empty

class Xlimit(object):

    def __init__(self, id_operator, limit, offset=0 ,eddies=2):
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
        if int(offset) > 0:
            self.offset = int(offset)
        else:
            self.offset = 0
        self.limit = int(limit) + self.offset
        self.done = False

    def __str__(self):
        return "XLimit"


    # Executes the XLimit.
    def execute(self, inputs, out):
        # Initialize input and output queues.
        self.left = inputs[0]
        self.qresults = out

        # Get the tuples (solution mappings) from the input queue.
        count = 0
        # LIMIT and OFFSET
        while count <= self.limit:

            try:
                # Get tuple (with solution mapping).
                self.probing.value = 1
                tuple1 = self.left.get(self.wait)

                # Perform projection in the solution mapping.
                if tuple1.data != "EOF":
                    # Create solution mapping with the specified domain in self.vars.
                    pass
                else:
                    # Stop limit once we have reached the end of the queue
                    # For example, when offset > total results
                    self.wait = False
                    break

                # Put tuple with solution mapping to output queue.
                tuple1.done = tuple1.done | pow(2, self.id_operator)
                if count >= self.offset:
                    self.qresults[self.eddy].put(tuple1)
                count += 1

            except Empty:
                pass
                #self.probing.value = 0

        # Done flag necessary for Eddy Operator to stop further production of tuples
        self.done = True

        tuple1.data = "EOF"
        tuple1.done = tuple1.done | pow(2, self.id_operator)
        self.qresults[self.eddy].put(tuple1)
        self.done = True