from nlde.operators.independent_operator import IndependentOperator
from nlde.planner.plan import Plan
from nlde.engine.contact_source import get_metadata
from nlde.planner.tree_plan import TreePlan
from nlde.operators.fjoin import Fjoin
from nlde.operators.xnjoin import Xnjoin
from nlde.operators.xproject import Xproject
from nlde.operators.xdistinct import Xdistinct
from nlde.operators.xunion import Xunion
from nlde.operators.dependent_operator import DependentOperator
from crop.query_plan_optimizer.logical_plan import LogicalPlan, LogicalUnion
from nlde.query import UnionBlock, JoinBlock, Optional
from crop.query_plan_optimizer.physical_plan import PhysicalPlan
import math

class nLDE_Optimizer(object):

    def __init__(self, **kwargs):

        self.eddies = kwargs.get("eddies")
        self.sources = kwargs.get("sources")
        self.poly = True

        # Planning requests
        self.planning_requests = 0

    def __str__(self):
        params = self.params
        return "\t".join(params)

    @property
    def params(self):
        params = []
        return params

    @property
    def params_dct(self):
        params = {
            "optimizer" : "nLDE",
            "poly" : str(self.poly)
        }
        return params

    def create_plan(self, query):
        self.query = query
        logical_plan = self.get_logical_plan(query.body)
        physical_plan = PhysicalPlan(self.sources, self.eddies, logical_plan, query, poly_operator=self.poly)
        physical_plan.planning_requests = self.planning_requests
        return physical_plan

    def estimate_card(self, l, r):
        return math.sqrt(((l * l) + (r * r)) / 2.0)
        #return l + r

    def get_logical_plan(self, body):

        if isinstance(body, UnionBlock):
            subplans = []
            for ggp in body.triples:
                subplan = self.get_logical_plan(ggp)
                if subplan:
                    subplans.append(subplan)
            if len(subplans) == 1:
                # No need for an additional union here
                return subplans[0]
            else:
                return LogicalUnion(subplans, Xunion)

        elif isinstance(body, JoinBlock):
            if body.bgp:
                l_plan = self.optimize_subquery(body.triples)
            elif len(body.triples) == 1:
                return self.get_logical_plan(body.triples[0])
            else:
                left_plan = self.get_logical_plan(body.triples[0])
                right_plan = self.get_logical_plan(body.triples[1])
                l_plan = LogicalPlan(left_plan, right_plan, Fjoin)
            return l_plan

        elif isinstance(body, Optional):
            plan = self.get_logical_plan(body.triples)
            return plan

    def optimize_subquery(self, triples):

        subtrees = []
        for triple in triples:
            # For each server, we need one requests to get the metadata
            self.planning_requests += len(self.sources)
            get_metadata(self.sources, triple)
            leaf = LogicalPlan(triple)
            subtrees.append(leaf)

        subtrees.sort(key=lambda  x: x.cardinality)

        stars = []
        while len(subtrees) > 0:

            to_delete = []
            star_tree = subtrees.pop(0)
            star_vars = star_tree.variables


            for j in range(0, len(subtrees)):
                subtree_j = subtrees[j]
                join_variables = set(star_vars).intersection(subtree_j.variables)

                # Case: There is a join.
                if len(join_variables) > 0:

                    to_delete.append(subtree_j)

                    # Place physical operator estimating cardinality.
                    if star_tree.is_triple_pattern:

                        res = self.estimate_card(star_tree.cardinality, subtree_j.cardinality)
                        # Place a Nested Loop join.
                        # Paper; if (tpi.count / tpi.pagesize) <= s.count then
                        if star_tree.cardinality < (subtree_j.cardinality / 100.0):
                            join_type = Xnjoin

                            res = 1

                        # Place a Symmetric Hash join.
                        else:
                            join_type = Fjoin

                    else:
                        # TODO: new change here
                        res = self.estimate_card(star_tree.cardinality, subtree_j.cardinality)
                        #print star_tree.cardinality, subtree_j.cardinality, res

                        if (star_tree.cardinality / float(subtree_j.cardinality) < 0.30) or (subtree_j.cardinality > 100*1000 and star_tree.cardinality < 100*1000) or (subtree_j.cardinality < 100*5):
                            join_type = Xnjoin

                            res = 1
                        else:
                            join_type = Fjoin

                    star_tree = LogicalPlan(star_tree, subtree_j, join_type)
                    star_tree.cardinality = res

            # Add current tree to the list of stars and
            # remove from the list of subtrees to process.
            stars.append(star_tree)
            for elem in to_delete:
                subtrees.remove(elem)

        # Stage 2: Build bushy tree to combine SSGs with common variables.
        while len(stars) > 1:

            subtree_i = stars.pop(0)
            star_vars = subtree_i.variables

            for j in range(0, len(stars)):
                subtree_j = stars[j]

                join_variables = set(star_vars).intersection(subtree_j.variables)

                # Case: There is a join between stars.
                if len(join_variables) > 0:

                    stars.pop(j)

                    res = self.estimate_card(star_tree.cardinality, subtree_j.cardinality)
                    # Place physical operators between stars.
                    if subtree_j.is_triple_pattern:

                        # This case models a satellite, therefore apply cardinality estimation.
                        if subtree_i.cardinality < (subtree_j.cardinality / 100.0):
                            join_type = Xnjoin
                        else:
                            join_type = Fjoin
                    else:
                        res = (subtree_i.cardinality + subtree_j.cardinality) / 2
                        join_type = Fjoin

                    star_tree = LogicalPlan(subtree_i, subtree_j, join_type)
                    star_tree.cardinality = res
                    stars.append(star_tree)

                    break


        tree = stars.pop()
        return tree


    # Not used: Just for reference
    def create_plan_original(self, query, eddies, source):

        # Plan structures.
        tree_height = 0
        id_operator = 0
        operators = []
        operators_desc = {}
        plan_order = {}
        operators_vars = {}
        ordered_subtrees = []
        independent_sources = 0
        eofs_operators_desc = {}
        operators_sym = {}
        sources_desc = {}
        eofs_desc = {}
        subtrees = []

        # Create initial signatures and leaves of the plan.
        for subquery in query.where.left.triple_patterns:
            sources_desc.update({id_operator: 0})
            eofs_desc.update({id_operator: 0})
            leaf = IndependentOperator(id_operator, source, subquery, sources_desc, subquery.get_variables(), eddies, eofs_desc)
            leaf.total_res = get_metadata(leaf.server, leaf.query)
            subtrees.append(leaf)
            ordered_subtrees.append(leaf.total_res)
            id_operator += 1

        # Order leaves depending on the cardinality of fragments.
        keydict = dict(zip(subtrees, ordered_subtrees))
        subtrees.sort(key=keydict.get)

        # Stage 1: Generate left_plan-linear index nested stars.
        stars = []
        id_operator = 0
        while len(subtrees) > 0:

            to_delete = []
            star_tree = subtrees.pop(0)
            star_vars = star_tree.vars
            tree_height = 0
            independent_sources = independent_sources + 1

            for j in range(0, len(subtrees)):
                subtree_j = subtrees[j]
                join_variables = set(star_vars) & set(subtree_j.join_vars)
                all_variables = set(star_tree.vars) | set(subtree_j.vars)

                # Case: There is a join.
                if len(join_variables) > 0:

                    to_delete.append(subtree_j)

                    # Update signatures.
                    sources = {}
                    sources.update(star_tree.sources)
                    sources.update(subtree_j.sources)
                    operators_desc[id_operator] = {}
                    operators_vars[id_operator] = join_variables
                    eofs_operators_desc[id_operator] = {}

                    # The current tree is the left_plan argument of the plan.
                    for source in star_tree.sources.keys():
                        if len(set(sources[source]) & join_variables) > 0:
                            # TODO: Change the next 0 for len of something
                            operators_desc[id_operator].update({source: 0})
                        sources_desc[source] = sources_desc[source] | pow(2, id_operator)

                        # TODO: check this.
                        eofs_operators_desc[id_operator].update({source: 0})
                        eofs_desc[source] = eofs_desc[source] | pow(2, id_operator)

                    # The subtree j is the right_plan argument of the plan.
                    for source in subtree_j.sources.keys():
                        if len(set(sources[source]) & join_variables) > 0:
                            # TODO: Change the next q for len of something
                            operators_desc[id_operator].update({source: 1})
                        sources_desc[source] = sources_desc[source] | pow(2, id_operator)

                        # TODO: check this.
                        eofs_operators_desc[id_operator].update({source: 1})
                        eofs_desc[source] = eofs_desc[source] | pow(2, id_operator)

                    plan_order[id_operator] = tree_height
                    operators_vars[id_operator] = join_variables
                    tree_height = tree_height + 1

                    # Place physical operator estimating cardinality.
                    if isinstance(star_tree, IndependentOperator):
                        res = self.estimate_card(star_tree.total_res, subtree_j.total_res)
                        # Place a Nested Loop join.
                        if star_tree.total_res < (subtree_j.total_res / 100.0):
                            subtree_j = DependentOperator(subtree_j.sources, subtree_j.server, subtree_j.query,
                                                          subtree_j.sources_desc, subtree_j.vars, subtree_j.total_res)
                            op = Xnjoin(id_operator, join_variables, eddies)
                            operators.append(op)
                            star_tree = TreePlan(op, all_variables, join_variables, sources,
                                                 star_tree, subtree_j, tree_height, 0)
                            operators_sym.update({id_operator: False})

                        # Place a Symmetric Hash join.
                        else:
                            op = Fjoin(id_operator, join_variables, eddies)
                            operators.append(op)
                            star_tree = TreePlan(op, all_variables, join_variables, sources,
                                                 star_tree, subtree_j, tree_height, res)
                            independent_sources = independent_sources + 1
                            operators_sym.update({id_operator: True})
                    else:
                        # TODO: new change here
                        res = self.estimate_card(star_tree.total_res, subtree_j.total_res)
                        #res = (2.0 * star_tree.total_res * subtree_j.total_res) / (star_tree.total_res + subtree_j.total_res)
                        #res = (star_tree.total_res + subtree_j.total_res) / 2
                        if (star_tree.total_res / float(subtree_j.total_res) < 0.30) or (subtree_j.total_res > 100*1000 and star_tree.total_res < 100*1000) or (subtree_j.total_res < 100*5):
                            subtree_j = DependentOperator(subtree_j.sources, subtree_j.server, subtree_j.query,
                                                          subtree_j.sources_desc, subtree_j.vars, subtree_j.total_res)
                            op = Xnjoin(id_operator, join_variables, eddies)
                            operators.append(op)
                            star_tree = TreePlan(op, all_variables, join_variables, sources,
                                                 star_tree, subtree_j, tree_height)
                            operators_sym.update({id_operator: False})
                        else:
                            op = Fjoin(id_operator, join_variables, eddies)
                            operators.append(op)
                            star_tree = TreePlan(op, all_variables,
                                                 join_variables, sources, star_tree, subtree_j, tree_height, res)
                            independent_sources = independent_sources + 1
                            operators_sym.update({id_operator: True})
                    id_operator += 1

            # Add current tree to the list of stars and
            # remove from the list of subtrees to process.
            stars.append(star_tree)
            for elem in to_delete:
                subtrees.remove(elem)

        # Stage 2: Build bushy tree to combine SSGs with common variables.
        while len(stars) > 1:

            subtree_i = stars.pop(0)

            for j in range(0, len(stars)):
                subtree_j = stars[j]

                all_variables = set(subtree_i.vars) | set(subtree_j.vars)
                join_variables = set(subtree_i.join_vars) & set(subtree_j.join_vars)

                # Case: There is a join between stars.
                if len(join_variables) > 0:

                    # Update signatures.
                    sources = {}
                    sources.update(subtree_i.sources)
                    sources.update(subtree_j.sources)

                    operators_desc[id_operator] = {}
                    operators_vars[id_operator] = join_variables
                    eofs_operators_desc[id_operator] = {}

                    for source in subtree_i.sources.keys():
                        # This models the restriction: a tuple must have the join
                        # variable instantiated to be routed to a certain join.
                        if len(set(sources[source]) & join_variables) > 0:
                            # TODO: Change the next 0 for len of something
                            operators_desc[id_operator].update({source: 0})
                        sources_desc[source] = sources_desc[source] | pow(2, id_operator)

                        # TODO: Check this.
                        eofs_operators_desc[id_operator].update({source: 0})
                        eofs_desc[source] = eofs_desc[source] | pow(2, id_operator)

                    for source in subtree_j.sources.keys():
                        # This models the restriction: a tuple must have the join
                        # variable instantiated to be routed to a certain join.
                        if len(set(sources[source]) & join_variables) > 0:
                            # TODO: Change the next 1 for len of something
                            operators_desc[id_operator].update({source: 1})
                        sources_desc[source] = sources_desc[source] | pow(2, id_operator)

                        # TODO: Check this.
                        eofs_operators_desc[id_operator].update({source: 1})
                        eofs_desc[source] = eofs_desc[source] | pow(2, id_operator)

                    plan_order[id_operator] = max(subtree_i.height, subtree_j.height)
                    stars.pop(j)

                    # Place physical operators between stars.
                    if isinstance(subtree_j, IndependentOperator):
                        res = self.estimate_card(star_tree.total_res , subtree_j.total_res)

                        # This case models a satellite, therefore apply cardinality estimation.
                        if subtree_i.total_res < (subtree_j.total_res/100.0):
                            subtree_j = DependentOperator(subtree_j.sources, subtree_j.server, subtree_j.query,
                                                          subtree_j.sources_desc, subtree_j.vars, subtree_j.total_res)
                            op = Xnjoin(id_operator, join_variables, eddies)
                            operators.append(op)
                            stars.append(TreePlan(op, all_variables,
                                                  join_variables, sources, subtree_i, subtree_j,
                                                  max(subtree_i.height, subtree_j.height, res)))
                            # Adjust number of asynchronous leaves.
                            independent_sources = independent_sources - 1
                            operators_sym.update({id_operator: False})
                        else:
                            op = Fjoin(id_operator, join_variables, eddies)
                            operators.append(op)
                            stars.append(TreePlan(op, all_variables, join_variables,
                                                  sources, subtree_i, subtree_j,
                                                  max(subtree_i.height, subtree_j.height, res)))
                            operators_sym.update({id_operator: True})
                    else:
                        res = (subtree_i.total_res + subtree_j.total_res) / 2
                        op = Fjoin(id_operator, join_variables, eddies)
                        operators.append(op)
                        stars.append(TreePlan(op, all_variables, join_variables,
                                              sources, subtree_i, subtree_j,
                                              max(subtree_i.height, subtree_j.height, res)))
                        operators_sym.update({id_operator: True})
                    id_operator += 1
                    break

            if len(subtrees) % 2 == 0:
                tree_height += 1

        tree_height += 1
        tree = stars.pop()


        # Adds the projection operator to the plan.
        if query.projection:
            op = Xproject(id_operator, query.projection, eddies)
            operators.append(op)
            tree = TreePlan(op,
                            tree.vars, tree.join_vars, tree.sources, tree, None, tree_height+1, tree.total_res)

            # Update signature of tuples.
            operators_sym.update({id_operator: False})
            operators_desc[id_operator] = {}
            eofs_operators_desc[id_operator] = {}
            for source in tree.sources:
                operators_desc[id_operator].update({source: 0})
                eofs_operators_desc[id_operator].update({source: 0})
                eofs_desc[source] = eofs_desc[source] | pow(2, id_operator)
                sources_desc[source] = sources_desc[source] | pow(2, id_operator)
            plan_order[id_operator] = tree_height
            operators_vars[id_operator] = tree.vars
            id_operator += 1
            tree_height += 1

        # Adds the distinct operator to the plan.
        if query.distinct:
            op = Xdistinct(id_operator, eddies)
            operators.append(op)
            tree = TreePlan(op, tree.vars, tree.join_vars, tree.sources,
                            tree, None, tree_height + 1, tree.total_res)

            # Update signature of tuples.
            operators_sym.update ({id_operator: False})
            operators_desc[id_operator] = {}
            eofs_operators_desc[id_operator] = {}
            for source in tree.sources:
                operators_desc[id_operator].update({source: 0})
                eofs_operators_desc[id_operator].update({source: 0})
                eofs_desc[source] = eofs_desc[source] | pow(2, id_operator)
                sources_desc[source] = sources_desc[source] | pow(2, id_operator)
            plan_order[id_operator] = tree_height
            operators_vars[id_operator] = tree.vars
            id_operator += 1
            tree_height += 1

        physical_plan = Plan(query_tree=tree, tree_height=tree.height,
                                  operators_desc=operators_desc, sources_desc=sources_desc,
                                  plan_order=plan_order, operators_vars=operators_vars,
                                  independent_sources=independent_sources,
                                  operators_sym=operators_sym, operators=operators)

        return physical_plan