"""
Created on Nov 24, 2011

@author: Maribel Acosta
"""
import random
from policy import Policy


class UniformRandomPolicy(Policy):
    """
    Implements a routing policy that routes tuples following a uniform random distribution, i.e.,
    all the eligible operators have the same probability to be chosen to process the tuple.
    """

    def __init__(self):
        self.priority_table = {}
        
    def initialize_priorities(self, plan_order):
        self.priority_table = plan_order

    def select_operator(self, operators, operators_desc, tup=None, operators_vars=None, operators_not_sym=[]):

        candidates = list(set(operators) - set(operators_not_sym))

        # List of selectable operators contain non-cartesian product routes.
        operators_selectable = []
        for o in candidates:
            if set(tup.sources) & set(operators_desc[o].keys()):
                operators_selectable.append(o)

        # The next operators is a dependent operator.
        if len(operators_selectable) == 0:
            return operators[0]

        # Randomly select an operator.
        i = random.randint(0, len(operators_selectable)-1)
        selected_operator = operators_selectable[i]

        return selected_operator
    
    def update_priorities(self, tup, queue=-1):
        pass
