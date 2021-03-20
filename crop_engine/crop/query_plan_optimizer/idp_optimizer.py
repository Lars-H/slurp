from itertools import combinations, chain
from nlde.engine.contact_source import get_metadata
from crop.query_plan_optimizer.physical_plan import PhysicalPlan
from nlde.operators.xnjoin import Xnjoin
from nlde.operators.fjoin import Fjoin
from nlde.operators.xunion import Xunion
from crop.query_plan_optimizer.logical_plan import LogicalPlan, LogicalUnion
from nlde.query import TriplePattern, UnionBlock, JoinBlock, Optional

import logging
logger = logging.getLogger("nlde_debug")

class IDP_Optimizer(object):

    def __init__(self, **kwargs):

        self.eddies = kwargs.get("eddies")
        self.sources = kwargs.get("sources")
        self.cost_model = kwargs.get("cost_model")
        self.robust_model = kwargs.get("robust_model")

        self.k = kwargs.get("k", 3)
        self.top_t = kwargs.get("top_t", 5)
        self.adaptive_k = kwargs.get("adaptive_k", False)

        self.poly = True

        # Allow for robust plans to be chose
        self.enable_robustplan = kwargs.get("enable_robustplan", True)
        self.robustness_threshold = kwargs.get("robustness_threshold", 0.1)
        self.cost_threshold = kwargs.get("cost_threshold", 0.3)

        # Runtime value
        self.robust_over_cost = False
        self.cost_robust_ratio = None
        self.cost_cost_ratio = None

        # Planning requests
        self.planning_requests = 0

    def __str__(self):
        return "\t".join([str(param) for param in self.params])

    @property
    def params(self):
        params = [
            self.robustness_threshold, self.cost_robust_ratio,
            self.cost_threshold, self.cost_cost_ratio,
            self.robust_over_cost,
            self.k, self.top_t, self.adaptive_k, self.enable_robustplan,
        ]
        return params

    @property
    def params_dct(self):
        params = {
            "optimizer" : "IDP",
            "poly" : str(self.poly),
            "robustness_threshold": self.robustness_threshold,
            "robustness_p_star": self.cost_robust_ratio,
            "cost_threshold": self.cost_threshold,
            "cost_p_star_cost_r_star_ratio": self.cost_cost_ratio,
            "r_star_selected": self.robust_over_cost,
            "idp_k": self.k,
            "idp_top_t": self.top_t,
            "idp_adaptive_k": self.adaptive_k
        }
        return params

    def true_subset(self, iterable):
        "true_subset([1,2,3]) --> () (1,) (2,) (3,) (1,2) (1,3) (2,3)"
        s = list(iterable)
        return chain.from_iterable(combinations(s, r) for r in range(1, len(s) + 1))

    def joinPlans(self, L, R, best=False, operators=[Fjoin, Xnjoin]):
        join_plans = []

        for operator in operators:

            # For Xnjoin: Either side must be a triple pattern to be the dependent operator in the plan
            if operator == Xnjoin:
                if L.is_triple_pattern or R.is_triple_pattern:
                    plan = LogicalPlan(L, R, operator)
                    cost = plan.compute_cost(self.cost_model)
                    plan.cost = cost
                    join_plans.append(plan)
            else:
                plan = LogicalPlan(L, R, operator)
                cost = plan.compute_cost(self.cost_model)
                plan.cost = cost
                join_plans.append(plan)

        if best:
            best_join = sorted(join_plans, key=lambda x: x.cost)[0]
            return best_join
        else:
            # If the leafs are triple patterns, the cheapest plan can be selected only
            if L.is_triple_pattern and R.is_triple_pattern:
                best_join = sorted(join_plans, key=lambda x: x.cost)[0]
                return [best_join]
            else:
                return join_plans

    def prune_plans(self, plans):
        best_plan = None
        best_cost = float("inf")
        for plan in plans:
            cost = plan.cost
            if cost < best_cost:
                best_plan = plan
                best_cost = cost
        return best_plan

    def best_n_plans(self, plans, n):
        return set(sorted(plans, key=lambda x : x.cost)[:n])

    def create_plan(self, query):
        logical_plan = self.get_logical_plan(query.body) #[0]
        physical_plan = PhysicalPlan(self.sources, self.eddies, logical_plan, query, poly_operator=self.poly)
        physical_plan.planning_requests = self.planning_requests
        return physical_plan

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
                l_plan = self.iterative_dynamic_programming1(body.triples)
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

    def iterative_dynamic_programming1(self, triple_patterns):

        if len(triple_patterns) == 1:
            # For each server, we need one requests to get the metadata
            self.planning_requests += len(self.sources)
            get_metadata(self.sources, triple_patterns[0])
            return LogicalPlan(triple_patterns[0])

        best_row = False
        opt_plan = {}
        toDo = set()

        k = min(len(triple_patterns), self.k)
        if self.adaptive_k and len(triple_patterns) >= 6:
            k = 2

        for index, triple_pattern in enumerate(triple_patterns):
            # For each server, we need one requests to get the metadata
            self.planning_requests += len(self.sources)
            get_metadata(self.sources, triple_pattern)
            accessPlan = set([LogicalPlan(triple_pattern)])
            opt_plan[(triple_pattern,)] = accessPlan
            toDo.add(triple_pattern)

        while len(toDo) > 1:
            k = min(k, len(toDo))
            for i in range(2, k + 1):
                for S in combinations(toDo, i):

                    opt_plan[S] = set()

                    for O in self.true_subset(S):

                        try:
                            opt_plan_O = opt_plan[O]
                            S_minus_O = tuple(set(S).difference(set(O)))

                            opt_plan_S_minus_O = opt_plan.get(S_minus_O, None)
                            if not opt_plan_S_minus_O or not opt_plan_O:
                                continue

                            for opt_plan_o in opt_plan_O:

                                for opt_plan_s_minus_o in opt_plan_S_minus_O:
                                    join_vars = opt_plan_o + opt_plan_s_minus_o
                                    if join_vars > 0:
                                        join_plans = self.joinPlans(opt_plan_o, opt_plan_s_minus_o)
                                        join_plans_S = opt_plan[S].union(join_plans)
                                        opt_plan[S] = self.best_n_plans(list(join_plans_S), self.top_t)
                                        #opt_plan[S] = join_plans_S

                        except Exception as e:
                            raise e

            best_plans = []
            V = set()
            for key, values in opt_plan.items():
                for value in values:
                    k_len = len(key)
                    if k_len == k and value and set(key).issubset(toDo):
                        V.add(key)
                        rob = value.cost
                        best_plans.append((value, value.cost, rob, key))

            if len(best_plans) == 0:
                raise Exception("IDP Error: No best plan")

            for v in V:
                del opt_plan[v]

            try:
                if len(best_plans) > 0:
                    # In intermediate steps of IDP: Take best plan only
                    best_plan = sorted(best_plans, key=lambda x: (x[1], x[2]))[0]

                    tps = best_plan[3]
                    opt_plan[(tps,)] = set([best_plan[0]])

                    if best_row:
                        best_plans.remove(best_plan)
                        # Best Row
                        for bp in best_plans:
                            if bp[3] == tps:
                                opt_plan[(tps,)].add(bp[0])

                    # Remove triple patterns from todo list
                    for tp in tps:
                        toDo.remove(tp)
                    toDo.add(best_plan[3])

            except Exception as e:
                raise e

        tmp_plans = []
        for plan in best_plans:
            cost = plan[0].cost
            rob = plan[0].average_cost(self.robust_model)
            tmp_plans.append((plan, cost, rob, cost/rob))

        cheap_plan = sorted(tmp_plans, key=lambda x: (x[1], x[3]))[0]

        # Decision rule for robust plan
        self.robust_over_cost = False
        rob_cost_ratio = cheap_plan[1] / cheap_plan[2]
        self.cost_robust_ratio = rob_cost_ratio
        if len(tmp_plans) > 1:
            tmp_plans.remove(cheap_plan)
            plans_over_thrshld = filter(lambda x: x[3] >= self.robustness_threshold, tmp_plans)
            if not plans_over_thrshld or len(plans_over_thrshld) == 0:
                plans_over_thrshld = tmp_plans
            robust_plan = sorted(plans_over_thrshld, key=lambda x: (x[1], x[2]))[0]
        else:
            robust_plan = cheap_plan


        # What is the cost ratio of the cheapest and the most robust plan
        cost_cost_ratio = cheap_plan[1] / robust_plan[1]
        self.cost_cost_ratio = cost_cost_ratio

        self.robust_over_cost = rob_cost_ratio <= self.robustness_threshold and cost_cost_ratio >= self.cost_threshold
        if self.enable_robustplan and self.robust_over_cost:
            logger.debug("IDP: Robust Plan over Cheapest Plan")
            return robust_plan[0][0]

        return cheap_plan[0][0]


