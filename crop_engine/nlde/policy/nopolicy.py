"""
Created on Nov 24, 2011

@author: Maribel Acosta
"""
from policy import Policy


class NoPolicy(Policy):
    """
    Implements a routing policy that follows the operators order indicated by the query plan.
    """

    def __init__(self):
        self.priority_table = {}
        
    def initialize_priorities(self, plan_order):
        self.priority_table = plan_order
        
    def select_operator(self, operators, operators_desc, tup=None, operators_vars=None, operators_not_sym=[]):

        selected_operator = operators[0]
        lowest_priority = float("inf")

        for operator in operators:
            priority = self.priority_table[operator]

            if priority < lowest_priority:
                if (tup.data == "EOF") or len(set(operators_vars[operator]) & set(tup.data.keys())) > 0:
                    selected_operator = operator
                    lowest_priority = priority
            
        return selected_operator
    
    def update_priorities(self, tup, queue=-1):
        pass
