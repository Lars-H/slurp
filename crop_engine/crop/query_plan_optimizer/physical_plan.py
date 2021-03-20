from nlde.operators.independent_operator import IndependentOperator
from nlde.planner.plan import Plan
#from nlde.engine.contactsources import get_metadata_tpf
from nlde.engine.contact_source import get_metadata
from nlde.planner.tree_plan import TreePlan
from nlde.query import TriplePattern, BGP
from nlde.operators.xnoptional import Xnoptional
from nlde.operators.xgoptional import Xgoptional
from nlde.operators.fjoin import Fjoin
from nlde.operators.polyfjoin import Poly_Fjoin
from nlde.operators.xnjoin import Xnjoin
from nlde.operators.xproject import Xproject
from nlde.operators.xdistinct import Xdistinct
from nlde.operators.xunion import Xunion
from nlde.operators.xlimit import Xlimit
from nlde.operators.xfilter import Xfilter
from nlde.operators.xorderby import Xorderby
from nlde.operators.polyxnjoin import Poly_Xnjoin
from nlde.operators.polybindjoin import Poly_Bind_Join
from nlde.operators.dependent_operator import DependentOperator
from crop.query_plan_optimizer.logical_plan import LogicalPlan, LogicalUnion

# Logging
import logging
logger = logging.getLogger("nlde_debug")

class PhysicalPlan(object):

    def __init__(self, source, eddies, logical_plan, query=None, poly_operator=True, **kwargs):
        self.source = source

        self.eddies = eddies
        self.physical_plan = None
        self.id_operator = 0
        self.sources_desc = {}
        self.eofs_desc = {}
        self.dependent_sources = 0
        self.independent_sources = 0
        self.operators_vars = {}
        self.operators = []
        self.operators_sym = {}
        self.sources = {}
        self.query = query

        self.poly = poly_operator
        self.sparql_limit = kwargs.get("sparql_limit", 500)

        self.brtpf_mappings = kwargs.get("brtpf_mappings", 30)
        self.sparql_mappings = kwargs.get("sparql_mappings", 50)
        # Maps each operator to the sources that pass through it
        # Also indicating if it is the left_plan (0) or right_plan (1) input
        self.operators_desc = {}
        self.plan_order = {}
        self.source_id = 0

        # Map operator ids to logical plan objects
        self.operator_id2logical_plan = {}

        # Requests stats
        self.execution_requests = 0
        self.planning_requests = 0

        # Maps sources to an integer representing the operators it passes through
        self.source_by_operator = {}

        self.logical_plan = logical_plan
        tree = self.from_logical_plan(logical_plan)

        self.sources_desc = self.source_by_operator

        # Adds the projection operator to the plan.
        if self.query and self.query.projection:
            tree = self.add_projection(tree)

        # Adds the distinct operator to the plan.
        if self.query and self.query.distinct:
            tree = self.add_distinct(tree)

        # Adds the order by operator to the plan.
        if self.query and len(self.query.order_by) > 0:
            tree = self.add_order_by(tree)

        # Adds the limit operator to the plan.
        if self.query and int(self.query.limit) > 0:
            tree = self.add_limit(tree)

        self.physical_plan = Plan(query_tree=tree, tree_height=tree.height,
                                  operators_desc=self.operators_desc, sources_desc=self.source_by_operator,
                                  plan_order=self.plan_order, operators_vars=self.operators_vars,
                                  independent_sources=self.independent_sources,
                                  operators_sym=self.operators_sym, operators=self.operators)

        self.independent_sources = self.physical_plan.independent_sources

        self.__average_cost = None
        self.__cardinality = {}


    def cost(self, cost_model):
        try:
            return self.logical_plan.compute_cost(cost_model)
        except:
            return -1

    def average_cost(self, cost_model):
        try:
            return self.logical_plan.average_cost(cost_model)
        except:
            return -1

    def __str__(self):
        return str(self.physical_plan.tree)

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other):
        return hash(self) == hash(other)

    def __len__(self):
        # Number of sources (i.e., triple patterns)
        return self.independent_sources + self.dependent_sources

    def cardinality(self, cost_model):
        return self.physical_plan.tree.compute_cardinality(cost_model.cardinality_estimation)

    @property
    def tree(self):
        return self.physical_plan.tree

    @property
    def is_bushy(self):
        return self.physical_plan.is_bushy

    @property
    def height(self):
        return self.maxDepth(self.tree)

    @property
    def total_requests(self):
        return self.planning_requests + self.execution_requests

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

    def traverse_tree(self, node):

        # Traverse through branches recursively and create json-seriazable dict while doing so
        branch = {}

        if isinstance(node, IndependentOperator) or isinstance(node, DependentOperator):
            branch['type'] = 'Leaf'
            branch['tpf'] = str(node.query)
            branch['cardinality'] = str(node.cardinality)
        else:
            if isinstance(node.operator, Xnjoin) or isinstance(node.operator, Poly_Xnjoin):

                # Get Produced Tuples
                op_id = node.operator.id_operator
                lp = self.operator_id2logical_plan.get(op_id, None)
                true_tuples = lp.true_cardinality if lp else -1
                estimated_tuples = lp.cardinality if lp else -1

                branch['produced_tuples'] = true_tuples
                branch['estimated_tuples'] = estimated_tuples
                branch['type'] = 'NLJ'
                if node.right is not None:
                    branch['right'] = self.traverse_tree(node.right)
                if node.left is not None:
                    branch['left'] = self.traverse_tree(node.left)
            elif isinstance(node.operator, Fjoin):

                # Get Produced Tuples
                op_id = node.operator.id_operator
                lp = self.operator_id2logical_plan.get(op_id, None)
                true_tuples = lp.true_cardinality if lp else -1
                estimated_tuples = lp.cardinality if lp else -1

                branch['produced_tuples'] = true_tuples
                branch['estimated_tuples'] = estimated_tuples
                branch['type'] = 'SHJ'
                if node.right is not None:
                    branch['right'] = self.traverse_tree(node.right)
                if node.left is not None:
                    branch['left'] = self.traverse_tree(node.left)
            else:
                return self.traverse_tree(node.left)
        return branch

    @property
    def json_dict(self):
        planDict = self.traverse_tree(self.tree)
        return planDict


    def logical_plan_stats(self, operator_dict):

        for key, value in self.operator_id2logical_plan.items():
            value.true_cardinality = operator_dict.get(key, -1)



    def join_subplans(self, left, right, join_type=None, card=-1, logial_plan=None):

        # Set Operator ID
        if logial_plan:
            self.operator_id2logical_plan[self.id_operator] = logial_plan
            logial_plan.operator_id = self.id_operator

        # Get Metadata for operator
        if isinstance(left, TriplePattern):
            # Get cardinality; Query only if necessary
            left_card = left.count if not left.count is None else get_metadata(self.source, left)
        else:
            left_card = left.total_res

        if isinstance(right, TriplePattern):
            # Get cardinality; Query only if necessary
            right_card = right.count if not right.count is None else get_metadata(self.source, right)
        else:
            right_card = right.total_res

        # Pre-decided Join Type
        if join_type:
            xn_join = True if (issubclass(join_type, Xnjoin) ) else False
            xn_optional = issubclass(join_type, Xnoptional)
            xg_optional = issubclass(join_type, Xgoptional)
            if xn_join or xn_optional:
                # Switch sides for NLJ
                if left_card > right_card:
                    tmp = left
                    left = right
                    right = tmp

        # Decide based in heursitics
        else:
            # Decide Join Type: xn = NLJ, FJ = SHJ
            if isinstance(left, IndependentOperator):
                xn_join = True if left_card < (right_card / 100.0) else False
            else:
                xn_join = True if left_card <= right_card else False

        # Joins Variable info
        join_vars = set(left.variables).intersection(right.variables)
        all_variables = set(left.variables).union(right.variables)



        # If the subplans have no varibale in common,
        # always place a Hash Join to handle the Cross-Product
        if len(join_vars) == 0:
            xn_join = False

        # Tree Plans as Leafs
        if isinstance(left, TreePlan):
            leaf_left = left
            for source in left.sources.keys():
                self.source_by_operator[source] = self.source_by_operator[source] | pow(2, self.id_operator)
                self.operators_desc.setdefault(self.id_operator, {})[source] = 0

            self.operators_desc.setdefault(self.id_operator, {})[self.source_id] = 0

        if isinstance(right, TreePlan):
            leaf_right = right
            for source in right.sources.keys():
                self.source_by_operator[source] = self.source_by_operator[source] | pow(2, self.id_operator)
                self.operators_desc.setdefault(self.id_operator, {})[source] = 1

        if xn_join and isinstance(left, TreePlan) and isinstance(right, TreePlan):
            print("Invalid plan")
        if xn_optional and isinstance(left, TreePlan) and isinstance(right, TreePlan):
            print("Invalid plan")

        # Operator Leafs
        if isinstance(left, TriplePattern) or isinstance(left, BGP):

            self.eofs_desc.update({self.source_id: 0})
            self.sources[self.source_id] = left.variables
            self.source_by_operator[self.source_id] = pow(2, self.id_operator)
            self.eofs_desc[self.source_id] = pow(2, self.id_operator)
            self.operators_desc.setdefault(self.id_operator, {})[self.source_id] = 0

            # Base on operator, create operator
            # If SHJ(FJ), use IO
            # Or if it is a NLJ(XN) and left_plan is a TP, then use IO
            if (not xn_join) or (xn_join and (isinstance(right, TriplePattern) or isinstance(right, BGP))):
                leaf_left = IndependentOperator(self.source_id, self.source, left,
                                                self.source_by_operator, left.variables, self.eddies,
                                                self.source_by_operator, sparql_limit= self.sparql_limit)
                self.independent_sources += 1

            elif (xn_join or xn_optional) and isinstance(right, TreePlan):
                leaf_left = DependentOperator(self.source_id, self.source, left,
                                              self.source_by_operator, left.variables, self.source_by_operator)
                self.dependent_sources += 1


            leaf_left.total_res = left_card
            self.source_id += 1

        if isinstance(right, TriplePattern) or isinstance(right, BGP):

            self.eofs_desc.update({self.source_id: 0})
            self.sources[self.source_id] = right.variables
            self.source_by_operator[self.source_id] = pow(2, self.id_operator)
            self.eofs_desc[self.source_id] = pow(2, self.id_operator)
            self.operators_desc.setdefault(self.id_operator, {})[self.source_id] = 1

            # Base on operator, create operator
            if xn_join or xn_optional:
                leaf_right = DependentOperator(self.source_id, self.source, right,
                                               self.source_by_operator, right.variables, self.source_by_operator)
                self.dependent_sources += 1

            else:
                leaf_right = IndependentOperator(self.source_id, self.source, right,
                                                 self.source_by_operator, right.variables, self.eddies,
                                                 self.source_by_operator, sparql_limit=self.sparql_limit)
                self.independent_sources += 1

            leaf_right.total_res = right_card
            self.source_id += 1

        self.operators_vars[self.id_operator] = join_vars

        self.plan_order[self.id_operator] = max(leaf_left.height, leaf_right.height)

        # Place Join
        if xn_join:  # NLJ
            #if isinstance(left, TreePlan) and isinstance(right, TriplePattern) and self.poly: # First condition only
            # needed for poly bind join
            if (isinstance(right, TriplePattern) or isinstance(right, BGP)) and self.poly:
                logger.debug("Placing Poly XN Join")
                op = Poly_Xnjoin(self.id_operator, join_vars, self.eddies, brtpf_mappings=self.brtpf_mappings,
                                 sparql_mappings=self.sparql_mappings)


                #logger.debug("Placing Poly Bind Join")
                #op = Poly_Bind_Join(self.id_operator, join_vars, self.eddies, left_card=card)
            else:
                logger.debug("Placing XN Join")
                op = Poly_Xnjoin(self.id_operator, join_vars, self.eddies, brtpf_mappings=1, sparql_mappings=1)
                #op = Xnjoin(self.id_operator, join_vars, self.eddies)


            self.operators_sym.update({self.id_operator: True})

            # If Right side has to be DP
            if not isinstance(leaf_right, DependentOperator):
                # Switch Leafs
                tmp = leaf_right
                leaf_right = leaf_left
                leaf_left = tmp

                # Update operators_descs for current operator id
                for key, value in self.operators_desc[self.id_operator].items():
                    # Leaf Right is now the DP and needs to be input Right, i.e. 1
                    if key == leaf_right.sources.keys()[0]:
                        self.operators_desc[self.id_operator][key] = 1
                    # All other will be on the left_plan input
                    else:
                        self.operators_desc[self.id_operator][key] = 0

        elif not xn_optional and not xg_optional:  # SHJ
            #op = Fjoin(self.id_operator, join_vars, self.eddies)
            if isinstance(left, TreePlan) and isinstance(right, TriplePattern) and self.poly:
                # Place Polymorphic Hash Join Operator
                op = Fjoin(self.id_operator, join_vars, self.eddies)
                #logger.debug("Placing Poly FJoin")
                #op = Poly_Fjoin(self.id_operator, join_vars, self.eddies, leaf_left, leaf_right)
            else:
                op = Fjoin(self.id_operator, join_vars, self.eddies)
            self.operators_sym.update({self.id_operator: False})

        elif not xg_optional: # XN Optional
            op = Xnoptional(self.id_operator, left.variables, right.variables, self.eddies)
            #op = Xnjoin(self.id_operator, join_vars, self.eddies)
            self.operators_sym.update({self.id_operator: True})

        else: # XG Optional
            op = Xgoptional(self.id_operator, left.variables, right.variables, self.eddies)
            self.operators_sym.update({self.id_operator: False})

        # Add Operator
        self.operators.append(op)

        tree_height = max(leaf_left.height, leaf_right.height) + 1
        #tree_sources = {k: v for k, v in self.sources.items()}
        # 2020-03-04: Changed here to route everything properly
        tree_sources = dict(leaf_left.sources)
        tree_sources.update(dict(leaf_right.sources))
        # Create Tree Plan
        join_card = card
        tree_plan = TreePlan(op, all_variables, join_vars, tree_sources,
                             leaf_left, leaf_right, tree_height, join_card)

        if isinstance(op, Xnjoin) and isinstance(leaf_left, TreePlan) and isinstance(leaf_right, TreePlan):
            raise Exception

        self.id_operator += 1
        return tree_plan


    def union_subplans(self, plans):

        op = Xunion(self.id_operator, self.eddies, inputs=len(plans))
        self.operators.append(op)

        union_vars = set()
        height = 0
        total_res = 0
        for plan in plans:
            union_vars.update(plan.variables)
            height = max(height, plan.height)
            total_res += plan.total_res
        height += 1


        tree_plan = TreePlan(op, union_vars, None, self.sources,
                             plans, None, height, total_res)

        # Update signature of tuples.
        self.operators_sym.update({self.id_operator: False})
        self.operators_desc[self.id_operator] = {}

        for source in tree_plan.sources:
            self.operators_desc[self.id_operator].update({source: 0})
            self.eofs_desc[source] = self.eofs_desc[source] | pow(2, self.id_operator)
            self.source_by_operator[source] = self.source_by_operator[source] | pow(2, self.id_operator)

        self.plan_order[self.id_operator] = tree_plan.height
        self.operators_vars[self.id_operator] = tree_plan.vars
        self.id_operator += 1

        return tree_plan


    def filter_subplan(self, tree, fltr):

        op = Xfilter(self.id_operator, self.eddies, filter=fltr)
        self.operators.append(op)
        tree = TreePlan(op,
                        tree.vars, tree.join_vars, tree.sources, tree, None, tree.height + 1, tree.total_res)

        # Update signature of tuples.
        self.operators_sym.update({self.id_operator: False})
        self.operators_desc[self.id_operator] = {}
        for source in tree.sources:
            self.operators_desc[self.id_operator].update({source: 0})
            self.eofs_desc[source] = self.eofs_desc[source] | pow(2, self.id_operator)
            self.source_by_operator[source] = self.source_by_operator[source] | pow(2, self.id_operator)

        self.plan_order[self.id_operator] = tree.height
        self.operators_vars[self.id_operator] = tree.vars
        self.id_operator += 1
        return tree


    def create_triple_pattern_plan(self, triple_pattern):

        self.eofs_desc.update({self.source_id: 0})
        self.sources[self.source_id] = triple_pattern.variables
        self.source_by_operator[self.source_id] = pow(2, self.id_operator)
        self.operators_desc.setdefault(self.id_operator, {})[self.source_id] = 0

        left = IndependentOperator(self.source_id, self.source, triple_pattern,
                                   self.source_by_operator, triple_pattern.variables, self.eddies,
                                   self.source_by_operator, sparql_limit=self.sparql_limit)
        left.total_res = triple_pattern.cardinality


        op = Xunion(self.id_operator, self.eddies, inputs=1)
        self.operators.append(op)

        # Update signature of tuples.
        self.operators_sym.update({self.id_operator: False})
        self.operators_desc[self.id_operator] = {}

        source = self.source_id
        self.operators_desc[self.id_operator].update({source: 0})

        self.eofs_desc[source] = self.eofs_desc[source] | pow(2, self.id_operator)
        self.source_by_operator[source] = self.source_by_operator[source] | pow(2, self.id_operator)

        self.plan_order[self.id_operator] = 1
        self.operators_vars[self.id_operator] = left.vars

        tree_plan = TreePlan(op, left.vars, left.vars, self.sources,
                             left, None, 1, left.total_res)

        self.source_id += 1
        self.independent_sources += 1
        self.id_operator += 1
        return tree_plan


    def add_projection(self, tree):
        op = Xproject(self.id_operator, self.query.projection, self.eddies)
        self.operators.append(op)
        tree = TreePlan(op,
                        tree.vars, tree.join_vars, tree.sources, tree, None, tree.height + 1, tree.total_res)

        # Update signature of tuples.
        self.operators_sym.update({self.id_operator: False})
        self.operators_desc[self.id_operator] = {}
        for source in tree.sources:
            self.operators_desc[self.id_operator].update({source: 0})
            self.eofs_desc[source] = self.eofs_desc[source] | pow(2, self.id_operator)
            self.sources_desc[source] = self.sources_desc[source] | pow(2, self.id_operator)
        self.plan_order[self.id_operator] = tree.height
        self.operators_vars[self.id_operator] = tree.vars
        self.id_operator += 1
        return tree


    def add_distinct(self, tree):
        op = Xdistinct(self.id_operator, self.eddies)
        self.operators.append(op)
        tree = TreePlan(op, tree.vars, tree.join_vars, tree.sources,
                        tree, None, tree.height + 1, tree.total_res)

        # Update signature of tuples.
        self.operators_sym.update({self.id_operator: False})
        self.operators_desc[self.id_operator] = {}
        for source in tree.sources:
            self.operators_desc[self.id_operator].update({source: 0})
            self.eofs_desc[source] = self.eofs_desc[source] | pow(2, self.id_operator)
            self.sources_desc[source] = self.sources_desc[source] | pow(2, self.id_operator)
        self.plan_order[self.id_operator] = tree.height
        self.operators_vars[self.id_operator] = tree.vars
        self.id_operator += 1
        return tree


    def add_limit(self, tree):
        op = Xlimit(self.id_operator, self.query.limit, self.query.offset, self.eddies)
        self.operators.append(op)
        tree = TreePlan(op,
                        tree.vars, tree.join_vars, tree.sources, tree, None, tree.height + 1, tree.total_res)

        # Update signature of tuples.
        self.operators_sym.update({self.id_operator: False})
        self.operators_desc[self.id_operator] = {}
        for source in tree.sources:
            self.operators_desc[self.id_operator].update({source: 0})
            self.eofs_desc[source] = self.eofs_desc[source] | pow(2, self.id_operator)
            self.sources_desc[source] = self.sources_desc[source] | pow(2, self.id_operator)
        self.plan_order[self.id_operator] = tree.height
        self.operators_vars[self.id_operator] = tree.vars
        self.id_operator += 1
        return tree


    def add_order_by(self, tree):
        op = Xorderby(self.id_operator, self.eddies, self.query.order_by)
        self.operators.append(op)
        tree = TreePlan(op, tree.vars, tree.join_vars, tree.sources,
                        tree, None, tree.height + 1, tree.total_res)

        # Update signature of tuples.
        self.operators_sym.update({self.id_operator: False})
        self.operators_desc[self.id_operator] = {}
        for source in tree.sources:
            self.operators_desc[self.id_operator].update({source: 0})
            self.eofs_desc[source] = self.eofs_desc[source] | pow(2, self.id_operator)
            self.sources_desc[source] = self.sources_desc[source] | pow(2, self.id_operator)
        self.plan_order[self.id_operator] = tree.height
        self.operators_vars[self.id_operator] = tree.vars
        self.id_operator += 1
        return tree


    def create_subplan(self, left, right):

        if isinstance(left, TriplePattern) and isinstance(right, TriplePattern):
            return self.join_subplans(left, right)

        elif isinstance(left, TriplePattern):
            return self.join_subplans(left, self.create_subplan(right[0], right[1]))

        elif isinstance(right, TriplePattern):
            return self.join_subplans(self.create_subplan(left[0], left[1]), right)
        else:
            return self.join_subplans(self.create_subplan(left[0], left[1]), self.create_subplan(right[0], right[1]))


    def create_subplan_by_join(self, left, right, join_type):
        if isinstance(left, TriplePattern) and isinstance(right, TriplePattern):
            return self.join_subplans(left, right, join_type=join_type)

        elif isinstance(left, TriplePattern):
            return self.join_subplans(left, self.create_subplan_by_join(right[0], right[1], right[2]), join_type=join_type)

        elif isinstance(right, TriplePattern):
            return self.join_subplans(self.create_subplan_by_join(left[0], left[1], left[2]), right, join_type=join_type)
        else:
            return self.join_subplans(self.create_subplan_by_join(left[0], left[1], left[2]), self.create_subplan_by_join(right[0], right[1], right[2]), join_type=join_type)


    def from_logical_plan(self, logical_plan):
        if isinstance(logical_plan, LogicalPlan):
            if len(logical_plan.filters) > 0:
                return self.filter_subplan(self.from_logical_join(logical_plan[0], logical_plan[1], logical_plan[2]), logical_plan.filters[0])
            else:
                return self.from_logical_join(logical_plan[0], logical_plan[1], logical_plan[2],
                                              logical_plan.cardinality, logial_plan=logical_plan)
        elif isinstance(logical_plan, LogicalUnion):
            subplans = []
            for subplan in logical_plan.subplans:
                subplans.append(self.from_logical_plan(subplan))
            return self.union_subplans(subplans)

    def from_logical_join(self, left, right, join_type, card=0, logial_plan=None):
        if isinstance(left, TriplePattern) or isinstance(left, BGP):
            return self.create_triple_pattern_plan(left)

        elif left.is_triple_pattern and right.is_triple_pattern:
            return self.join_subplans(left[0], right[0], join_type=join_type, card=card, logial_plan=logial_plan)

        elif left.is_triple_pattern:
            return self.join_subplans(left[0], self.from_logical_plan(right), join_type=join_type, card=card,
                                      logial_plan=logial_plan)

        elif right.is_triple_pattern:
            return self.join_subplans(self.from_logical_plan(left), right[0], join_type=join_type, card=card,
                                      logial_plan=logial_plan)
        else:
            return self.join_subplans(self.from_logical_plan(left), self.from_logical_plan(right),
                                      join_type=join_type, card=card, logial_plan=logial_plan)