#from nlde.planner.optimizer import TreePlan
from nlde.planner.tree_plan import TreePlan
from nlde.operators.independent_operator import IndependentOperator
from nlde.operators.dependent_operator import DependentOperator

class Plan(object):
    def __init__(self, **kwargs):
        """
        Init plan generic object

        self.tree, tree_height, self.operators_desc, self.sources_desc,
         plan_order, operators_vars, independent_sources, self.eofs_operators_desc,
         operators_sym, operators
        """
        self.tree = kwargs.get("query_tree")
        self.tree_height = kwargs.get("tree_height")
        self.operators_desc = kwargs.get("operators_desc")
        self.sources_desc = kwargs.get("sources_desc")
        #self.sources_desc = {int(key): value for key, value in self.sources_desc.items()}

        self.plan_order = kwargs.get("plan_order")
        #self.plan_order = {int(key): value for key, value in self.plan_order.items()}

        self.operators_vars = kwargs.get("operators_vars")
        #self.operators_vars = {int(key): set([str(val) for val in value]) for key, value in self.operators_vars.items()}

        self.independent_sources = kwargs.get("independent_sources")

        self.operators_desc = kwargs.get("operators_desc")
        #self.operators_desc = { int(key) : {int(k2) : val2 for k2, val2 in value.items() } for key, value in self.operators_desc.items()}

        self.operators_sym = kwargs.get("operators_sym")
        #self.operators_sym = {str(key): value for key, value in kwargs.get("operators_sym").items()}

        # Init Operators of Plan by inspecting its tree
        self.operators = []
        self.get_operators(self.tree)

        self.robustness_val= None
        self.operators = kwargs.get("operators")
        #self.operators = { str(key) : value for key, value in kwargs.get("operators").items()}



    def __str__(self):
        return str(self.tree)

    @property
    def is_bushy(self):
        if isinstance(self.tree.left, TreePlan) and isinstance(self.tree.right, TreePlan):
            return True
        return False

    def cost(self, cost_model):
        return self.tree.compute_cost(cost_model)

    def robustness(self, cost_model):
        return None

    def cardinality(self, cost_model):
        return self.tree.compute_cardinality(cost_model.cardinality_estimation)

    @property
    def height(self):
        return self.maxDepth(self.tree)

    def maxDepth(self, node):
        if isinstance(node, IndependentOperator) or isinstance(node, DependentOperator) :
            return 0

        else:

            # Compute the depth of each subtree
            lDepth = self.maxDepth(node.left)
            rDepth = self.maxDepth(node.right)

            # Use the larger one
            if (lDepth > rDepth):
                return lDepth + 1
            else:
                return rDepth + 1

    def get_operators(self, obj):
        if isinstance(obj, TreePlan):
            self.operators.append(obj.operator)
            self.get_operators(obj.left)
            self.get_operators(obj.right)

    def to_dict(self):
        return {
            "query_tree" : self.tree.to_dict(),
            "tree_height" : self.tree_height,
            "operators_desc" : self.operators_desc,
            "sources_desc" : self.sources_desc,
            "plan_order" : self.plan_order,
            "operators_vars" : {key : list(val) for key, val in self.operators_vars.items()},
            "independent_sources" : self.independent_sources,
            "operators_sym" : self.operators_sym,
            #"operators": [operator.to_dict() for operator in self.operators],
            "type" : "Plan",
            "robustness": self.robustness_val
        }