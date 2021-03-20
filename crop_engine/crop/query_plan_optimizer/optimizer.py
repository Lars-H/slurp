from nlde.operators.fjoin import Fjoin
from nlde.operators.xunion import Xunion
from crop.query_plan_optimizer.logical_plan import LogicalPlan, LogicalUnion
from nlde.query import UnionBlock, JoinBlock, Optional

class Optimizer(object):


    def union_subplans(self, subplans):
        if len(subplans) == 1:
            return subplans[0]
        else:
            return LogicalUnion(subplans, Xunion)


    def get_logical_plan_simple(self, body):

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
                l_plan = self.optimize_bgp(body.triples)
            elif len(body.triples) == 1:
                return self.get_logical_plan(body.triples[0])
            elif len(body.triples) == 2 and isinstance(body.triples[1], Optional):
                # Get operator for Optional
                # TODO: Handle case with several optionals
                left_plan = self.get_logical_plan(body.triples[0])
                right_plan =  self.get_logical_plan(body.triples[1])
                operator = self.get_optional_operator(left_plan,right_plan)
                l_plan = LogicalPlan(left_plan, right_plan, operator)
                return l_plan
            else:
                left_plan = self.get_logical_plan(body.triples[0])
                right_plan =  self.get_logical_plan(body.triples[1])
                l_plan = LogicalPlan(left_plan, right_plan, Fjoin)
            return l_plan

        elif isinstance(body, Optional):
            plan = self.get_logical_plan(body.triples)
            return plan

        return None

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
                l_plan = self.optimize_bgp(body.triples)
            elif len(body.triples) == 1:
                return self.get_logical_plan(body.triples[0])
            elif len(body.triples) == 2 and isinstance(body.triples[1], Optional):
                # Get operator for Optional
                # TODO: Handle case with several optionals
                left_plan = self.get_logical_plan(body.triples[0])
                right_plan =  self.get_logical_plan(body.triples[1])
                operator = self.get_optional_operator(left_plan,right_plan)
                l_plan = LogicalPlan(left_plan, right_plan, operator)
                return l_plan

            elif len(body.triples) == 2:
                left_plan = self.get_logical_plan(body.triples[0])
                right_plan =  self.get_logical_plan(body.triples[1])
                l_plan = LogicalPlan(left_plan, right_plan, Fjoin)
                return l_plan

            elif len(body.triple_patterns) > 1:
                left_join_block = JoinBlock(body.triple_patterns, filters=body.filters)
                left_plan = self.get_logical_plan(left_join_block)
                opts = body.optionals
                right_plan = self.get_logical_plan(JoinBlock(opts))
                operator = self.get_optional_operator(left_plan, right_plan)
                l_plan = LogicalPlan(left_plan, right_plan, operator)
                return l_plan

            else:
                opts = body.optionals
                if len(opts) == 1:
                    return self.get_logical_plan(opts.pop())

                opt1 = opts.pop()
                left_plan = self.get_logical_plan(opt1)
                right_plan = self.get_logical_plan(JoinBlock(opts))
                operator = self.get_optional_operator(left_plan, right_plan)
                l_plan = LogicalPlan(left_plan, right_plan, operator)
                return l_plan

            return l_plan

        elif isinstance(body, Optional):
            plan = self.get_logical_plan(body.triples)
            return plan

        return None

    def create_plan(self, query):
        raise NotImplementedError

    def optimize_bgp(self, triple_patterns):
        raise NotImplementedError

    def get_optional_operator(self, left_plan, right_plan):
        raise NotImplementedError

    def get_physical_join_operator(self, left_plan, right_plan):
        raise NotImplementedError