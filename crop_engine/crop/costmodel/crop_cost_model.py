from nlde.operators.independent_operator import IndependentOperator
from nlde.operators.dependent_operator import DependentOperator
from nlde.operators.xnjoin import Xnjoin
from nlde.operators.fjoin import Fjoin
from nlde.operators.xproject import Xproject
from nlde.operators.xdistinct import Xdistinct
from nlde.operators.xunion import Xunion
from crop.costmodel.cardinality_estimation import CardinalityEstimation

class CropCostModel(object):

    def __init__(self,**kwargs):

        self.cardinality_estimation = CardinalityEstimation()

        self.__cost_functions = {
            DependentOperator: self.access_operator,
            IndependentOperator: self.access_operator,
            Xnjoin: self.nested_loop_join,
            Fjoin: self.hash_join,
            Xproject: self.project,
            Xdistinct: self.distinct,
            Xunion: self.union
        }

        self.__shj_io_const = float(kwargs.get("shj_io",  0.001))
        self.__nlj_io_const = float(kwargs.get("nlj_io",  0.001))
        self.__nlj_height_factor = float(kwargs.get("height_discount",  4.0))
        self.__nlj_dependent_discount_factor = float(kwargs.get("nlj_request_discount",  1.0))
        self.__func = None
        self.__switch = None
        self.__switch_function = None

    def __str__(self):
        return "Cost Model"


    def __hash__(self):
        return hash(str(self))

    @property
    def params(self):
        params = [
            self.__shj_io_const, self.__nlj_io_const, self.__nlj_height_factor, self.__nlj_dependent_discount_factor
        ]
        return params

    @property
    def params_dct(self):
        params = {
            "phi_shj": self.__shj_io_const, "phi_nlj": self.__nlj_io_const, "delta": self.__nlj_height_factor
        }
        return params

    def set_function(self, func):
        self.cardinality_estimation.set_function(func)
        self.__func = func

    def set_switch(self, switch, func):
        self.__switch = switch
        self.__switch_function = func

    @property
    def switch_function(self):
        return self.__switch_function

    @property
    def switch(self):
        return self.__switch

    @property
    def access_operator(self):

        def f(e1):
            return self.cardinality(e1)
        return f

    @property
    def hash_join(self):

        def f(e1, e2):
            e1.compute_cost(self)
            e2.compute_cost(self)

            # Cost for requesting/querying the data
            request_cost = 0

            # Cost for locally processing the operator results
            io_cost = 0

            if isinstance(e1, IndependentOperator) or e1.is_triple_pattern:
                request_cost += max([0.01 * self.cardinality(e1),1])

            if isinstance(e2, IndependentOperator) or e2.is_triple_pattern:
                request_cost +=max([0.01 * self.cardinality(e2),1])

            if self.switch:
                for switch in self.switch:
                    if e1 == switch.left and e2 == switch.right:
                        io_cost = self.cardinality_estimation.join_cardinality(e1, e2, self.switch_function)
            else:
                io_cost = self.cardinality_estimation.join_cardinality(e1, e2, self.__func)

            cost = request_cost + self.__shj_io_const * io_cost
            return cost

        return f

    @property
    def nested_loop_join(self):

        def f(e1, e2):
            e1.compute_cost(self)
            e2.compute_cost(self)

            height = max(e1.height, e2.height)

            # Cost for requesting/querying the data
            request_cost = 0

            # Cost for locally processing the operator results
            io_cost = 0

            # If it is at a leaf, then we have request cost
            if isinstance(e1, IndependentOperator) or e1.is_triple_pattern:
                request_cost += max([0.01 * self.cardinality(e1),1])

            # Height Discount factor:
            # The higher a NLJ placed in the tree, the more likely is that there are fewer results to be processed by it
            height_factor = max(1, self.__nlj_height_factor * (height + 1))

            if self.switch:
                for switch in self.switch:
                    if e1 == switch.left and e2 == switch.right:
                        io_cost = self.cardinality_estimation.join_cardinality(e1, e2, self.switch_function)
            else:
                io_cost = self.cardinality_estimation.join_cardinality(e1, e2, self.__func)


            request_cost += self.__nlj_dependent_discount_factor *  (max(self.cardinality(e1) , max(0.1*io_cost, 1) ) )/ height_factor

            cost = request_cost + self.__nlj_io_const * io_cost + self.__nlj_io_const * self.cardinality(e2)
            return cost

        return f

    @property
    def project(self):

        def f(e1, e2):
            e1_cost = e1.compute_cost(self)
            return e1_cost
        return f

    @property
    def distinct(self):

        def f(e1, e2):
            e1_cost = e1.compute_cost(self)
            return e1_cost
        return f

    @property
    def union(self):
        def f(e1, e2):
            e1_cost = e1.compute_cost(self)
            e2_cost = e2.compute_cost(self)
            return e1_cost + e2_cost
        return f

    @property
    def munion(self):
        def f(subplans):
            cost_sum = 0
            for subplan in subplans:
                cost_sum += subplan.compute_cost(self)
            return cost_sum
        return f

    def __getitem__(self, item):
        return self.__cost_functions[item]

    def cardinality(self, elem):
        return elem.cardinality

    def join_type(self, e1, e2):

        e1_vars = e1.variables_dict
        e2_vars = e2.variables_dict
        if len(set(e1_vars['s']).intersection(set(e2_vars['s']))) > 0:
            return 1
        elif len(set(e1_vars['s']).intersection(set(e2_vars['o']))) > 0:
            return 2
        elif len(set(e1_vars['o']).intersection(set(e2_vars['s']))) > 0:
            return 2
        elif len(set(e1_vars['o']).intersection(set(e2_vars['o']))) > 0:
            return 3
        return -1
