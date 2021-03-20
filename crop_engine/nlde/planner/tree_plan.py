from multiprocessing import Process
#from billiard import Process
from nlde.operators.xproject import Xproject
from nlde.operators.xdistinct import Xdistinct
from nlde.operators.xunion import Xunion
from nlde.operators.xlimit import Xlimit
from nlde.operators.xgoptional import Xgoptional
from nlde.operators.xnoptional import Xnoptional
from nlde.operators.polyfjoin import Poly_Fjoin
from nlde.operators.independent_operator import IndependentOperator

class TreePlan(object):
    """
    Represents a plan to be executed by the engine.

    It is composed by a left_plan node, a right_plan node, and an operators node.
    The left_plan and right_plan nodes can be leaves to contact sources, or subtrees.
    The operators node is a physical operators, provided by the engine.

    The execute() method evaluates the plan.
    It creates a process for every node of the plan.
    The left_plan node is always evaluated.
    If the right_plan node is an independent operators or a subtree, it is evaluated.
    """

    def __init__(self, operator, variables, join_vars, sources, left, right, height=0, res=0):
        self.operator = operator
        self.vars =  set([str(val) for val in variables])
        self.join_vars = join_vars #set([str(val) for val in join_vars])
        sources_dct =  {int(key): set([str(val) for val in value]) for key, value in sources.items()}
        self.sources = sources_dct
        self.left = left
        self.right = right
        self.height = height
        self.total_res = res
        self.p = None
        self.cost = None
        self.cardinality = None

    def __str__(self):
        if isinstance(self.operator, Xunion):
            if not isinstance(self.left, IndependentOperator):
                inner = " UNION ".join([str(subtree) for subtree in self.left])
            else:
                inner = str(self.left)
            return "({})".format(inner)
        elif isinstance(self.operator, Xnoptional) or isinstance(self.operator, Xgoptional):
            return "({} OPT {})".format(self.left, self.right)
        elif not self.right:
            return "({})".format(self.left)
        else:
            return "({} AND {})".format(self.left, self.right)

    @property
    def is_triple_pattern(self):
        return False

    def aux(self, n):
        # Node string representation.
        s = n + str(self.operator) + "\n" + n + str(self.vars) + "\n"

        # Left tree plan string representation.
        if self.left:
            s = s + str(self.left.aux(n + "  "))

        # Right tree plan string representation.
        if self.right:
            s = s + str(self.right.aux(n + "  "))

        return s

    @property
    def variables(self):
        return self.vars

    @property
    def join_variables(self):
        return self.join_vars

    @property
    def query(self):
        if self.right:
            return "{} {}".format(self.left.query, self.right.query)
        else:
            return self.left.query

    @property
    def variables_dict(self):
        v_dict = {
            "s" : set(),
            "p" : set(),
            "o" : set(),
        }
        if isinstance(self.operator, Xunion):
            for subtree in self.left:
                for key, value in subtree.variables_dict.items():
                    if value:
                        v_dict[key].update(value)
        else:
            for key, value in self.right.variables_dict.items():
                if value:
                    v_dict[key].update(value)
            for key, value in self.left.variables_dict.items():
                if value:
                    v_dict[key].update(value)
        return v_dict

    def compute_cardinality(self, card_est_model):
        if self.right:
            # If Join Operator
            self.cardinality = max(card_est_model.join_cardinality(self.left, self.right), 1.0)
        elif isinstance(self.operator, Xunion):
            self.cardinality = card_est_model.union_cardinality(self.left)
            return self.cardinality
        else:
            # If Project Operator
            self.cardinality =  self.left.compute_cardinality(card_est_model)
        return self.cardinality


    def compute_cost(self, cost_model):
        treeplan_cost = cost_model[type(self.operator)]

        if isinstance(self.operator, Xproject) or isinstance(self.operator, Xdistinct) or isinstance(self.operator, Xlimit):
            self.cardinality = self.left.cardinality

        elif isinstance(self.operator, Xunion):
            self.cardinality = cost_model.cardinality_estimation.union_cardinality(self.left)
        else:
            if cost_model.switch and self in cost_model.switch: #and self.join_type >= 2:
                self.cardinality = cost_model.cardinality_estimation.join_cardinality(self.left, self.right, func=cost_model.switch_function)
            else:
                self.cardinality = cost_model.cardinality_estimation.join_cardinality(self.left, self.right)

        cost_self = treeplan_cost(self.left, self.right)

        if isinstance(self.left, TreePlan):
            cost_self += self.left.cost
        if isinstance(self.right, TreePlan):
            cost_self += self.right.cost

        self.cost = cost_self
        return self.cost

    @property
    def join_type(self):
        if not self.right:
            return -1

        e1_vars = self.left.variables_dict
        e2_vars = self.right.variables_dict
        if len(set(e1_vars['s']).intersection(set(e2_vars['s']))) > 0:
            return 1
        elif len(set(e1_vars['s']).intersection(set(e2_vars['o']))) > 0:
            return 2
        elif len(set(e1_vars['o']).intersection(set(e2_vars['s']))) > 0:
            return 2
        elif len(set(e1_vars['o']).intersection(set(e2_vars['o']))) > 0:
            return 3
        return -1

    def execute(self, operators_input_queues,  eddies_queues, p_list, operators_desc):

        operator_inputs = operators_input_queues[self.operator.id_operator]
        # Execute left_plan sub-plan.
        if self.left:
            # Case: Plan leaf (asynchronous).
            if self.left.__class__.__name__ == "IndependentOperator":
                try:
                    q = operators_desc[self.operator.id_operator][self.left.sources.keys()[0]]
                    p1 = Process(target=self.left.execute,
                                 args=(operators_input_queues[self.operator.id_operator][q], None, eddies_queues, p_list,))
                    p1.start()
                    p_list.put(p1.pid)
                except Exception as e:
                    raise e

            # Case: Tree plan.
            elif self.left.__class__.__name__ == "TreePlan":
                p1 = Process(target=self.left.execute,
                             args=(operators_input_queues, eddies_queues, p_list, operators_desc,))

                p1.start()
                p_list.put(p1.pid)

            # Case: Array of independent operators or Tree Plan.
            else:
                for elem in self.left:
                    if not isinstance(elem, TreePlan):
                        q = operators_desc[self.operator.id_operator][elem.sources.keys()[0]]
                        p1 = Process(target=elem.execute,
                                     args=(operators_input_queues[self.operator.id_operator][q], None, eddies_queues,
                                           p_list,))

                    else:
                        # q = operators_desc[self.operator.id_operator][elem.sources.keys()[0]]
                        p1 = Process(target=elem.execute,
                                     args=(operators_input_queues, eddies_queues, p_list, operators_desc,))

                    p1.start()
                    p_list.put(p1.pid)


        # Execute right_plan sub-plan.
        if self.right and ((self.right.__class__.__name__ == "TreePlan") or (self.right.__class__.__name__ == "IndependentOperator")):

            # Case: Plan leaf (asynchronous).
            if self.right.__class__.__name__ == "IndependentOperator":
                q = operators_desc[self.operator.id_operator][self.right.sources.keys()[0]]
                p2 = Process(target=self.right.execute,
                             args=(None, operators_input_queues[self.operator.id_operator][q], eddies_queues, p_list,))

            # Case: Tree plan.
            else:
                p2 = Process(target=self.right.execute,
                             args=(operators_input_queues, eddies_queues, p_list, operators_desc,))


            p2.start()
            p_list.put(p2.pid)

            # Pass the Process ID to Join operator if it is a Poly Join
            if isinstance(self.operator, Poly_Fjoin):
                self.operator.right_pid = p2.pid

            #right_plan = operators_input_queues[self.operator.id_operator]

        # Right sub-plan. Case: Plan leaf (dependent).
        else:
        # TODO: Change this. Uncomment line below
        #elif self.right_plan:
            operator_inputs = operator_inputs + [self.right]
            #print self.operator, operator_inputs
            #right_plan = self.right_plan

        # Create a process for the operator node.

        self.p = Process(target=self.operator.execute,
                         args=(operator_inputs, eddies_queues,))

        self.p.start()
        p_list.put(self.p.pid)