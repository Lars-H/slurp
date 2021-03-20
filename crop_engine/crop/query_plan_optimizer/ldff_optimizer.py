from crop.query_plan_optimizer.optimizer import Optimizer
from crop.decomposition.ldff_decomposer import LDFF_Decomposer
from crop.query_plan_optimizer.physical_plan import PhysicalPlan
from nlde.operators.xnjoin import Xnjoin
from nlde.operators.fjoin import Fjoin
from nlde.operators.xnoptional import Xnoptional
from nlde.operators.xgoptional import Xgoptional
from crop.query_plan_optimizer.logical_plan import LogicalPlan, LogicalUnion
from crop.costmodel.cardinality_estimation import CardinalityEstimation
from nlde.engine.contact_source import get_metadata
import math
from nlde.query import TriplePattern, Filter, BGP


import logging
logger = logging.getLogger("nlde_debug")


class LDFF_Optimizer(Optimizer):

    def __init__(self, **kwargs):

        self.eddies = kwargs.get("eddies")
        self.sources = kwargs.get("sources")

        self.poly = kwargs.get("pbj", True)
        self.decomposer = kwargs.get("decomposer", True)
        self.prune_sources = kwargs.get("pruning", True)


        self.cardinality_estimation = CardinalityEstimation()

        # TPF Config
        self.tpf_page_size = kwargs.get("tpf_page_size", 100)

        # brTPF Config
        self.brtpf_mapping_cnt = kwargs.get("brtpf_mappings", 30)

        # SPARQL Config
        self.sparql_limit = 10000 #kwargs.get("sparql_limit", 1000)
        self.sparql_mapping_cnt = kwargs.get("sparql_mappings", 50)

        # Logging the number of BGPs
        self.bgp_count = []

        # Completeness
        self.decompostion_completeness = []

        # Cost
        self.decompostion_cost = []
        self.max_cost = 0

        # Planning requests
        self.planning_requests = 0

    def __repr__(self):
        return "LDFF Optimizer"

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
            "optimizer" : "ldff",
            "pbj" : str(self.poly),
            "brTPF_max_bindings" : str(self.brtpf_mapping_cnt),
            "sparql_max_bindings": str(self.sparql_mapping_cnt),
            "sparql_page_size" : str(self.sparql_limit),
            "cardinality_estimation" : str(self.cardinality_estimation),
            "bgps"  :  sum(self.bgp_count) / len(self.bgp_count),
            "prune_sources" : str(self.prune_sources),
            "completeness" : sum(self.decompostion_completeness) / len(self.decompostion_completeness) ,
            "decomposition_cost" : sum(self.decompostion_cost) / len(self.decompostion_cost),
            "max_cost" : str(self.max_cost)
        }

        if self.decomposer:
            params['decomposer'] = "merge"
        else:
            params['decomposer'] = "None"

        return params


    def create_plan(self, query):
        self.query = query
        logical_plan = self.get_logical_plan(query.body)
        physical_plan = PhysicalPlan(self.sources, self.eddies, logical_plan, query, poly_operator=self.poly,
                                     sparql_limit=self.sparql_limit, sparql_mappings=self.sparql_mapping_cnt,
                                     brtpf_mappings=self.brtpf_mapping_cnt)
        physical_plan.planning_requests = self.planning_requests
        return physical_plan

    def optimize_bgp(self, triple_patterns):

        for tp in triple_patterns:
            if isinstance(tp, TriplePattern):
                # For each server, we need one requests to get the metadata
                self.planning_requests += len(self.sources)
                get_metadata(self.sources, tp)

        # Compute E_star for completeness and max cost
        E_star = self.compute_completeness(triple_patterns)
        self.max_cost = self.compute_cost(triple_patterns)

        # Prune Sources
        if self.prune_sources:
            triple_patterns = self.prune_relevant_source(triple_patterns)

        # Compute E and completness

        E = self.compute_completeness(triple_patterns)
        try:
            bgp_comp  = E / E_star
        except ZeroDivisionError:
            bgp_comp= 1.0

        self.decompostion_completeness.append(bgp_comp)

        # Decomposition
        if self.decomposer:
            # Compute Decomposition
            decomposition = LDFF_Decomposer.get_decomposition(triple_patterns)
        else:
            decomposition = triple_patterns

        # Compute Cost
        try:
            bgp_cost = self.compute_cost(decomposition) / self.max_cost
        except ZeroDivisionError:
            bgp_cost = 1.0

        self.decompostion_cost.append(bgp_cost)

        # Get plan
        plan = self.decompostion_to_plan(decomposition)
        return plan



    def decompostion_to_plan(self, decomposition):
        access_plans = []
        filters = []
        for subplan in decomposition:
            if isinstance(subplan, Filter):
                filters.append(subplan)
            else:
                if isinstance(subplan, BGP):
                    access_plans.append(LogicalPlan(subplan))
                else:
                    access_plans.append(LogicalPlan(subplan))
                self.bgp_count.append(float(len(subplan)))

        todo = sorted(access_plans, key=lambda x: x.cardinality)
        plan = todo[0]
        todo.remove(plan)

        root = True

        while len(todo):
            for i in range(len(todo)):
                if len(plan.variables.intersection(todo[i].variables)) > 0:

                    join_operator = self.get_physical_join_operator(plan, todo[i])

                    if root and plan.is_basic_graph_pattern and join_operator == Xnjoin:
                        plan = LogicalUnion([plan])

                    plan = LogicalPlan(plan, todo[i], join_operator)
                    plan.compute_cardinality(self.cardinality_estimation)
                    todo.remove(todo[i])
                    root = False
                    break
            else:
                # In case we cannot find another join able triple pattern
                next_tp = todo[0]
                join_operator = self.get_physical_join_operator(plan, next_tp)
                plan = LogicalPlan(plan, next_tp, join_operator)
                plan.compute_cardinality(self.cardinality_estimation)
                todo.remove(next_tp)

        plan.filters = filters
        return plan


    def get_optional_operator(self, left_plan, right_plan):

        xn_requests = left_plan.cardinality
        xg_requests = math.ceil(right_plan.cardinality / self.tpf_page_size)

        # Decide which optional Operator to place
        if len(right_plan.triple_patterns) == 1 and xn_requests < xg_requests:
            return Xnoptional
        else:
            return Xgoptional


    def get_physical_join_operator(self, left_plan, right_plan):

        if isinstance(right_plan, LogicalUnion):
            nlj_sum = 0
            hj_sum = 0
            for union_subplan in right_plan.subplans:
                nlj_requests, hj_requests  = self.get_requests(union_subplan.left, left_plan.cardinality)
                nlj_sum += nlj_requests
                hj_sum += hj_requests
        else:
            nlj_requests, hj_requests  = self.get_requests(right_plan.left, left_plan.cardinality)

        logger.debug("NLJ requests: {}; HJ requests: {}".format(nlj_requests, hj_requests))

        if right_plan.is_triple_pattern and not right_plan.is_basic_graph_pattern and  right_plan.left.variable_position == 5:
            if right_plan.left[1].value == "<http://www.w3.org/2002/07/owl#sameAs>":
                return Xnjoin

        if nlj_requests <= hj_requests:
            return Xnjoin
        else:
            return Fjoin

    def get_requests(self, plan, card_left):

        request_sum_nlj = 0
        request_sum_hj = 0
        for source, card in plan.sources.items():
            if source.startswith("sparql@"):
                if self.poly:
                    request_sum_nlj += math.ceil(float(card_left) / float(self.sparql_mapping_cnt))
                else:
                    request_sum_nlj += card_left
                request_sum_hj += math.ceil(float(card) / float(self.sparql_limit))

            elif source.startswith("brtpf@"):
                if self.poly:
                    request_sum_nlj += math.ceil(float(card_left) / float(self.brtpf_mapping_cnt))
                else:
                    request_sum_nlj += card_left
                request_sum_hj += math.ceil(float(card) /float(self.tpf_page_size))

            else:
                request_sum_nlj += math.ceil(float(card_left))
                request_sum_hj += math.ceil(float(card) /float(self.tpf_page_size))

        return request_sum_nlj, request_sum_hj

    def prune_relevant_source(self, triple_patterns):

        source2tp = {}
        stars = {}
        todo = []
        for triple_pattern in triple_patterns:
            if isinstance(triple_pattern, TriplePattern):
                if triple_pattern.subject.isvariable():
                    stars.setdefault(str(triple_pattern[0]), []).append(triple_pattern)
                for source in triple_pattern.sources.keys():
                    source2tp.setdefault(source, []).append(triple_pattern)
                todo.append(triple_pattern)

        if len(source2tp.keys()) == 0:
            return triple_patterns

        # Assign maximum mathcing source
        tp2source = {}
        while len(todo) > 0:
            for source in sorted(source2tp, key=lambda k: len(source2tp[k]), reverse=True):
                tps = source2tp[source]
                for tp in tps:
                    if tp in todo:
                        tp2source.setdefault(tp, []).append(source)
                        todo.remove(tp)

        tp2removable = {}
        for tp in triple_patterns:
            if isinstance(tp, TriplePattern):
                pruned_sources =  set(tp2source[tp])
                remove_sources = set(tp.sources.keys()) - pruned_sources
                tp2removable[tp] = remove_sources


        # Do not remove if they occure commonly in a star
        for star in stars.values():
            # Only stars with at least two tps
            if len(star) < 2:
                continue
            common_sources = set()
            for tp in star:
                if isinstance(tp, TriplePattern):
                    if len(common_sources) == 0:
                        common_sources = set(tp.sources.keys())
                    else:
                        common_sources = common_sources.intersection(set(tp.sources.keys()))

            for tp in star:
                if isinstance(tp, TriplePattern):
                    tp2removable[tp] = tp2removable[tp] - common_sources

        # Finally remove sources from triple patterns source dict
        for tp in triple_patterns:
            if isinstance(tp, TriplePattern):
                for src in tp2removable[tp]:
                    del tp.sources[src]

        return triple_patterns

    def compute_cost(self, subqueries):

        S_sum = 0
        for subquery in subqueries:
            if isinstance(subquery, TriplePattern) or isinstance(subquery, BGP):
                S_sum += len(subquery.sources.keys())

        return float(S_sum)


    def compute_completeness(self, triple_patterns):

        n = 0.0
        k = 0.0
        for tp in triple_patterns:
            if isinstance(tp, TriplePattern):
                n += 1.0
                k += float(len(tp.sources.keys()))

        completeness = 0.5 * n * (n-1.0) + k
        return completeness
