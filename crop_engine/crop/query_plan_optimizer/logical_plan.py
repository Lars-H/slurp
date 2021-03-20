#from nlde.util.querystructures import TriplePattern
from nlde.query import TriplePattern, BGP
from nlde.operators.xnjoin import Xnjoin
from nlde.operators.fjoin import  Fjoin
from nlde.operators.xunion import Xunion
from nlde.util.misc import median
from itertools import chain, combinations


class LogicalPlan(object):

    def __init__(self, L, R=None, operator=None, filters=[]):

        triple_patterns = set()

        self.__sources = set(L.source_set)
        self.__vars = L.variables
        if isinstance(L,TriplePattern):
            triple_patterns.add(L)
        else:
            triple_patterns = triple_patterns.union(L.triple_patterns)

        if R:
            self.__vars = self.__vars.union(R.variables)
            self.__sources.update(R.source_set)
            triple_patterns = triple_patterns.union(R.triple_patterns)

            # Place L and R for XNJoin correctly
            if operator == Xnjoin:
                #if isinstance(R, LogicalPlan) and L.is_triple_pattern:
                if not R.is_triple_pattern and L.is_triple_pattern:
                    tmp = L
                    L = R
                    R = tmp
            elif operator == Fjoin:
                if L.cardinality > R.cardinality:
                    tmp = L
                    L = R
                    R = tmp
        self.__operator = operator
        self.__L = L
        self.__R = R
        self.__filters = filters
        self.__cardinality = -1

        self.operator_id = -1
        self.true_cardinality = -1

        self.__triple_patterns = frozenset(triple_patterns)
        self.cost = None


    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other):
        return hash(self) == hash(other)

    def __str__(self):
        if self.is_triple_pattern:
            return str(self.left)
        else:
            return "({}, {}, {})".format(str(self.__L), str(self.__R), str(self.__operator))

    def __len__(self):
        return len(self.triple_patterns)

    def __add__(self, other):
        return len(self.variables.intersection(other.variables))

    @property
    def L(self):
        return self.__L

    @property
    def R(self):
        return self.__R

    @property
    def left(self):
        return self.__L

    @property
    def right(self):
        return self.__R

    @property
    def height(self):
        if self.is_triple_pattern:
            return 0
        else:
            # Compute the depth of each subtree
            lDepth = self.__L.height
            rDepth = self.__R.height

            # Use the larger one
            if (lDepth > rDepth):
                return lDepth + 1
            else:
                return rDepth + 1
    @property
    def variables(self):
        return self.__vars

    @property
    def variables_dict(self):
        if self.is_triple_pattern:
            return self.__L.variables_dict
        else:
            v_dict = {
                "s" : set(),
                "p" : set(),
                "o" : set(),
            }
            for key, value in self.__R.variables_dict.items():
                if value:
                    v_dict[key].update(value)
            for key, value in self.__L.variables_dict.items():
                if value:
                    v_dict[key].update(value)
            return v_dict

    @property
    def triple_patterns(self):
        return set(self.__triple_patterns)

    @property
    def filters(self):
        return self.__filters

    @filters.setter
    def filters(self, filters):
        self.__filters = filters

    @property
    def source_set(self):
        return self.__sources

    @property
    def is_triple_pattern(self):
        if not self.__R and not self.__operator:
            return True
        return False

    @property
    def is_basic_graph_pattern(self):
        if not self.__R and not self.__operator and isinstance(self.__L, BGP):
            return True
        return False

    @property
    def cardinality(self):
        if self.is_triple_pattern:
            return self.__L.cardinality
        elif self.__operator == Xunion:
            return self.__L.cardinality + self.__R.cardinality
        else:
            return self.__cardinality

    @cardinality.setter
    def cardinality(self, value):
        self.__cardinality = value

    @property
    def join_type(self):

        if self.is_triple_pattern or self.__operator == Xunion:
            return -1

        e1_vars = self.__L.variables_dict
        e2_vars = self.__R.variables_dict
        if len(set(e1_vars['s']).intersection(set(e2_vars['s']))) > 0:
            return 1
        elif len(set(e1_vars['s']).intersection(set(e2_vars['o']))) > 0:
            return 2
        elif len(set(e1_vars['o']).intersection(set(e2_vars['s']))) > 0:
            return 2
        elif len(set(e1_vars['o']).intersection(set(e2_vars['o']))) > 0:
            return 3
        return -1


    def compute_cardinality(self, cardinality_model):
        self.__cardinality = cardinality_model.join_cardinality(self.left, self.right)
        return self.__cardinality

    def compute_cost(self, cost_model):

        if self.is_triple_pattern:
            cost = cost_model.access_operator(self)
            return cost
        else:
            plan_cost_function = cost_model[self.__operator]
            if cost_model.switch and self in cost_model.switch:  # and self.join_type >= 2:
                self.__cardinality = cost_model.cardinality_estimation.join_cardinality(self.left, self.right,
                                                                                  func=cost_model.switch_function)
            else:
                self.__cardinality = cost_model.cardinality_estimation.join_cardinality(self.left, self.right)

            cost_self = plan_cost_function(self.__L, self.__R)
            if not self.L.is_triple_pattern:
                cost_self += self.L.cost
            if not self.R.is_triple_pattern:
                cost_self += self.R.cost
            self.cost = cost_self
            return self.cost

    def nodes(self, nodes):
        if self.__L.is_triple_pattern:
            if self.__R and self.__R.is_triple_pattern:
                nodes.append(self)
                return nodes
            else:
                nodes.append(self)
                r_nodes = self.__R.nodes(nodes)
                return r_nodes
        else:
            if self.__R.is_triple_pattern:
                nodes.append(self)
                l_nodes = self.__L.nodes(nodes)
                return  l_nodes
            else:
                l_nodes = self.__L.nodes(nodes)
                r_nodes = self.__R.nodes(l_nodes)
                return r_nodes

    def average_cost(self, cost_model):

        base_cost = self.cost #(cost_model)
        nodes_cost = [base_cost]
        s = self.nodes([])
        s = [n for n in s if n.join_type >= 2]
        node_subsets = chain.from_iterable(combinations(s, r) for r in range(1, len(s) + 1))
        for nodes in node_subsets:
            if len(nodes) > 0:
                cost_model.set_switch(nodes, lambda x,y: sum([x,y]))
                nodes_cost.append(self.compute_cost(cost_model))
                cost_model.set_switch(nodes, lambda x,y: max(x, y))
                nodes_cost.append(self.compute_cost(cost_model))


                cost_model.set_switch(nodes, lambda x,y: max(x / y, y / x))
                nodes_cost.append(self.compute_cost(cost_model))

                cost_model.set_switch(None, None)
                cost_model.set_function(None)

        self.__average_cost = median(nodes_cost)
        return self.__average_cost

    def __getitem__(self, item):
        return (self.__L, self.__R, self.__operator)[item]

    def compatible(self, T):
        return len((self.variables.intersection(T.variables))) > 0


class LogicalUnion(object):

    def __init__(self, subplans, operator=None):

        self.__vars = set()
        self.__sources = set()
        for subplan in subplans:
            self.__vars.update(subplan.variables)
            self.__sources.update(subplan.source_set)
        self.__operator = operator
        self.__subplans = subplans
        self.__operator = Xunion

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other):
        return hash(self) == hash(other)

    def __str__(self):
        inner = [str(subplan) for subplan in self.__subplans]
        inner.append(self.__operator)
        return str(tuple(inner))

    def __len__(self):
        return sum([len(subplan) for subplan in self.__subplans])

    def __getitem__(self, item):
        return self.__subplans[item]

    @property
    def is_triple_pattern(self):
        return False

    @property
    def subplans(self):
        return self.__subplans

    @property
    def cardinality(self):
        card_sum = 0
        for subplan in self.__subplans:
            card_sum += subplan.cardinality
        return card_sum

    @property
    def source_set(self):
        return self.__sources

    @property
    def variables(self):
        vars = set()
        for subplan in self.__subplans:
            vars.update(subplan.variables)
        return vars

    @property
    def triple_patterns(self):
        tps = set()
        for subplan in self.__subplans:
            tps.update(subplan.triple_patterns)
        return tps

    def compute_cardinality(self, cardinality_model):
        self.__cardinality = cardinality_model.union_cardinality(self.__subplans)
        return self.__cardinality

    def compute_cost(self, cost_model):
        plan_cost_function = cost_model[self.__operator]
        self.cost = plan_cost_function(self.__subplans)
        self.__cardinality = cost_model.cardinality_estimation.union_cardinality(self.__subplans)
        return self.cost

    def average_cost(self, cost_model):

        cost_sum = 0
        for subplan in self.__subplans:
            cost_sum += subplan.average_cost(cost_model)
        return cost_sum / len(self.__subplans)

