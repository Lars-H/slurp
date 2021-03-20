from itertools import product
from crop.query_plan_optimizer.physical_plan import PhysicalPlan
from nlde.operators.xnjoin import Xnjoin
from nlde.operators.fjoin import Fjoin
from nlde.operators.xunion import Xunion
from nlde.operators.xnoptional import Xnoptional
from nlde.operators.xgoptional import Xgoptional
from crop.query_plan_optimizer.logical_plan import LogicalPlan, LogicalUnion
from crop.source_selection.utils import StarSubquery
from crop.costmodel.cardinality_estimation import CardinalityEstimation
from crop.source_selection import NaiveSourceSelection, StarBasedSourceSelection, AskSourceSelector, \
    HybridSourceSelector, StatSourceSelector
from crop.statistics import FederationPredicateStatistic
import math
from nlde.query import UnionBlock, JoinBlock, Optional, Filter
from crop.query_plan_optimizer.optimizer import Optimizer


import logging
logger = logging.getLogger("nlde_logger")

PAGE_SIZE = 100

class Federated_Optimizer(Optimizer):

    def __init__(self, **kwargs):

        self.eddies = kwargs.get("eddies")
        self.sources = kwargs.get("sources")

        self.poly = True

        self.verbose = False
        self.stats = "sampled"
        self.source_selection_method = "naive"

        ## No Stats
        if self.stats == "none":
            self.source_selector = AskSourceSelector(sources=self.sources)
            self.predicate_stats = None

        ## Sample Stats
        elif self.stats == "sampled":
            stats_fn = "stats/predicate_stats_0_001.json"
            #stats_fn = "stats/predicate_stats_empty.json"
            self.predicate_stats = FederationPredicateStatistic(stats_fn)
            #self.source_selector = AskSourceSelector(sources=self.sources, predicate_stats=self.predicate_stats)
            self.source_selector = StatSourceSelector(sources=self.sources, predicate_stats=self.predicate_stats)

        ## Full Stats
        elif self.stats == "full":
            stats_fn = "stats/predicate_stats_full.json"
            self.predicate_stats = FederationPredicateStatistic(stats_fn)
            self.source_selector = AskSourceSelector(sources=self.sources)
            #self.source_selector = HybridSourceSelector(predicate_stats=self.predicate_stats)

        if self.stats != "none":
            logger.info("Statistics File: {}".format(stats_fn))
        else:
            logger.info("No Statistics")
        logger.info("Source Selection Approach: {}".format(self.source_selector))

        if self.source_selection_method == "naive":
            self.source_selection = NaiveSourceSelection
        else:
            self.source_selection = StarBasedSourceSelection

        self.cardinality_estimation = CardinalityEstimation()
        #self.cardinality_estimation.set_function(lambda x,y: x+y)

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
            "optimizer" : "leftdeep",
            "poly" : str(self.poly),
            "statistics" : str(self.predicate_stats),
            "source_selector" : str(self.source_selector),
            "source_optimizer" : str(self.source_selection_method),
            "verbose_plan" : str(self.verbose)
        }
        return params


    def union_subplans(self, subplans):
        if len(subplans) == 1:
            return subplans[0]
        else:
            return LogicalUnion(subplans, Xunion)

    def optimize_subquery(self, subquery, filters):

        plans = []
        for tp_combination in product(*subquery):

            access_plans = []
            for tp in tp_combination:
                access_plans.append(LogicalPlan(tp))

            todo = sorted(access_plans, key=lambda x: x.cardinality)
            plan = todo[0]
            todo.remove(plan)

            while len(todo):
                for i in range(len(todo)):
                    if len(plan.variables.intersection(todo[i].variables)) > 0:
                        plan = LogicalPlan(plan, todo[i], self.get_physical_join_operator(plan, todo[i]))
                        plan.compute_cardinality(self.cardinality_estimation)
                        todo.remove(todo[i])
                        break
                else:
                    # In case we cannot find another join able triple pattern
                    next_tp = todo[0]
                    plan = LogicalPlan(plan, next_tp, self.get_physical_join_operator(plan, next_tp))
                    plan.compute_cardinality(self.cardinality_estimation)
                    todo.remove(next_tp)

            plan.filters = filters
            plans.append(plan)
        if len(plans) == 0:
            return None
        plan = self.union_subplans(plans)
        return plan

    def get_physical_join_operator(self, left_plan, right_plan):

        # TODO: In case we have poly joins, we need to adjust the request number estimation

        left_cost = 0
        if left_plan.is_triple_pattern:
            left_cost = math.ceil(left_plan.cardinality / PAGE_SIZE)

        nlj_requests = left_cost + left_plan.cardinality * len(right_plan.source_set)
        hj_requests = left_cost + math.ceil(right_plan.cardinality / PAGE_SIZE)

        #if right_plan.is_triple_pattern and right_plan.left_plan.variable_position == 5:
        #    if right_plan.left_plan[1].value == "<http://www.w3.org/2002/07/owl#sameAs>":
        #        return Xnjoin

        if nlj_requests < hj_requests:
            #if not left_plan.is_triple_pattern and not right_plan.is_triple_pattern:
            #    return Fjoin
            return Xnjoin
        else:
            return Fjoin

    def optimize_bgp(self, triple_patterns):
        ssqs, filters = self.starshaped_subqueries(triple_patterns)

        source_groups = {}
        for ssq in ssqs.values():
            ssq.source_groups = self.source_selection.select_sources(ssq.triple_patterns, self.source_selector, verbose=self.verbose, mode=self.source_selection_method, predicate_stats=self.predicate_stats)
            source_groups[ssq.join_var] = ssq.source_groups

        if self.source_selection != NaiveSourceSelection and self.source_selection_method == "auth":
            self.source_selection.interstar_optimized_selection(ssqs.values())


        updated_source_groups = []
        for join_var, ssq in ssqs.items():
            # Update the source groups only, if we could find a relevant source for all triple patterns
            min_len = min([len(sg) for sg in ssq.source_groups])
            if min_len > 0:
                updated_source_groups.extend(ssq.source_groups)
            else:
                updated_source_groups.extend(source_groups[join_var])

        # Optimize Star Query
        plan = self.optimize_subquery(updated_source_groups, filters)

        #src_grps = self.groupby_sources(plan.triple_patterns)
        return plan

    def create_plan(self, query):
        self.query = query
        logical_plan = self.get_logical_plan(query.body)
        return
        physical_plan = PhysicalPlan(self.sources, self.eddies, logical_plan, query, poly_operator=self.poly)
        return physical_plan

    def starshaped_subqueries(self, triple_patterns):

        ssqs = {}
        filters = []
        for tp in triple_patterns:
            if isinstance(tp, Filter):
                filters.append(tp)
                continue
            var = str(tp[0])
            if var not in ssqs.keys():
                ssq = StarSubquery([tp], var)
                ssqs[var] = ssq
            else:
                ssqs[var].triple_patterns.append(tp)
        return ssqs, filters


    def get_optional_operator(self, left_plan, right_plan):
        xn_requests = math.ceil(left_plan.cardinality / PAGE_SIZE) + left_plan.cardinality
        xg_requests = math.ceil(left_plan.cardinality / PAGE_SIZE) + math.ceil(right_plan.cardinality / PAGE_SIZE)

        # Decide which optional Operator to place
        if len(right_plan.triple_patterns) == 1 and xn_requests < xg_requests:
            return Xnoptional
        else:
            return Xgoptional
        return l_plan


    def groupby_sources(self, triple_patterns):

        source_groups = {}
        for triple_pattern in triple_patterns:
            for source in triple_pattern.sources.keys():
                source_groups.setdefault(source, []).append(triple_pattern)

        return source_groups