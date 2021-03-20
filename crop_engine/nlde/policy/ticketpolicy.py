"""
Created on Nov 24, 2011

@author: Maribel Acosta
"""
from multiprocessing import Manager


class TicketPolicy(object):
    """
    Implements a routing policy that routes tuple to the most selective operators.
    Operators' selectivity is estimated as the ratio of the input / output monitored so far by the engine.
    """

    def __init__(self):
        manager = Manager()
        self.priority_table = manager.dict()
        self.initial_plan = manager.dict()
        
    def initialize_priorities(self, plan_order):
        
        self.initial_plan = plan_order
        
        for operator in plan_order.keys():
            self.priority_table.update({operator: OperatorRate(0.0, 0.0, 0.0)})
        
    def select_operator(self, operators, operators_desc, tup=None, operators_vars=None, operators_not_sym=[]):

        selected_operator = -1
        highest_priority = -2
        all_output_zero = True

        operators_selectable = list(set(operators) - set(operators_not_sym))

        if len(operators_selectable) == 1:
            return operators_selectable[0]
        
        if len(operators_selectable) == 0:
            return operators[0]

        for operator in operators_selectable:

            input_card = self.priority_table[operator].left + self.priority_table[operator].right

            # Case: 'operators' has received no input,
            # route to first operators according to plan.
            if input_card == 0:
                return operator

            # Check whether 'operators' has produced output.
            if self.priority_table[operator].output == 0:
                inverse_selectivity = -1
            else:
                inverse_selectivity = input_card / self.priority_table[operator].output
                all_output_zero = False

            if inverse_selectivity > highest_priority:
                selected_operator = operator
                highest_priority = inverse_selectivity
                
        # Case all operators have not produced results. Follow the query plan.
        if all_output_zero:
            selected_operator = operators[0]
            #height = self.initial_plan[selected_operator]
            #for operators in operators:
            #    if self.initial_plan[operators] == height+1:
            #        selected_operator = operators
            #        break

        return selected_operator

    def __str__(self):
        st = ""
        for op in self.priority_table.keys():
            st = st + "{" + str(op) + \
                 ":{L: "+ str(self.priority_table[op].left) + \
                 ", R:" + str(self.priority_table[op].right) + \
                 ", O: " + str(self.priority_table[op].output) + "}} "
    
        return st

    def update_priorities(self, tuple, queue=-1):

        # Update the output rate of the operators that produced 'tuple'.
        if tuple.from_operator != -1:
            self.priority_table[tuple.from_operator].output = self.priority_table[tuple.from_operator].output +1

        # Update the input rate of the operators where 'tuple' was routed to.
        if tuple.to_operator != -1:
            if queue == -1:
                self.priority_table[tuple.to_operator].left = self.priority_table[tuple.to_operator].left + 1
            else:
                self.priority_table[tuple.to_operator].right = self.priority_table[tuple.to_operator].right + 1


class OperatorRate(object):
    
    def __init__(self, left, right, output):
        self.left = left
        self.right = right
        self.output = output
