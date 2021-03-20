from nlde.operators.xnjoin import Xnjoin
from nlde.operators.fjoin import Fjoin
from nlde.engine.contact_source import get_metadata
from random import shuffle, choice
from crop.query_plan_optimizer.physical_plan import PhysicalPlan
from crop.query_plan_optimizer.logical_plan import LogicalPlan
#from crop.query_plan_optimizer.simple_logical_plan import SimpleLogicalPlan as LogicalPlan
from crop.costmodel.crop_cost_model import CropCostModel


from time import time

class GeneticOptimizer(object):

    def __init__(self, **kwargs):

        self.eddies = kwargs.get("eddies")
        self.source = kwargs.get("sources")
        self.cost_model = kwargs.get("cost_model",None)
        self.robust_model = kwargs.get("robust_model", None)

        self.tested_plans = set()

    @property
    def params_dct(self):
        return {}

    def random_plan(self, triples):
        leafs = set([0])
        nodes = 0
        edges = []
        out_edges = {}
        in_edges = {}
        while len(leafs) < len(triples):
            random_leaf = choice(list(leafs))
            out_edges[random_leaf] = (nodes + 1, nodes + 2)
            in_edges[nodes + 1] = random_leaf
            in_edges[nodes + 2] = random_leaf
            nodes += 1
            edges.append((nodes, random_leaf))
            leafs.add(nodes)
            nodes += 1
            edges.append((nodes, random_leaf))
            leafs.add(nodes)

            leafs.remove(random_leaf)

        for i in range(100):
            shuffle(triples)
            leaf_map = {key: value for key, value in zip(leafs, triples)}
            p = (out_edges, leaf_map)
            try:
                if not p in self.tested_plans:
                    self.tested_plans.add(p)
                    # TODO: Random plan repr hashale!
                    plan = self.plan_from_tree(0, out_edges, leafs, leaf_map)
                #plan = self.lw_plan_from_tree(0, out_edges, leafs, leaf_map)
            except Exception as e:
                #raise e
                continue
            return plan


    def plan_from_tree(self, node_id, out_edges, leafs, leaf_map):

        l = out_edges[node_id][0]
        r = out_edges[node_id][1]
        if l in leafs and r in leafs:
            tp_l = LogicalPlan(leaf_map[l])
            tp_r = LogicalPlan(leaf_map[r])
            if not leaf_map[l].compatible(leaf_map[r]):
                raise Exception
            plan = LogicalPlan(tp_l, tp_r, choice([Xnjoin, Fjoin]))
            plan.compute_cost(self.cost_model)
            return plan
        elif l in leafs:
            tp_l = LogicalPlan(leaf_map[l])
            plan = LogicalPlan(tp_l, self.plan_from_tree(r, out_edges, leafs, leaf_map), choice([Xnjoin, Fjoin]))
            plan.compute_cost(self.cost_model)
            return plan
        elif r in leafs:
            tp_r = LogicalPlan(leaf_map[r])
            plan = LogicalPlan(tp_r, self.plan_from_tree(l, out_edges, leafs, leaf_map), choice([Xnjoin, Fjoin]))
            plan.compute_cost(self.cost_model)
            return plan
        else:
            plan = LogicalPlan(self.plan_from_tree(l, out_edges, leafs, leaf_map), self.plan_from_tree(r, out_edges, leafs, leaf_map), Fjoin)
            plan.compute_cost(self.cost_model)
            return plan


    def lw_plan_from_tree(self, node_id, out_edges, leafs, leaf_map, prefix=""):
        l = out_edges[node_id][0]
        r = out_edges[node_id][1]
        operator = choice([Xnjoin, Fjoin])

        lid = prefix + "001"
        rid = prefix + "010"

        if operator == Xnjoin:
            lid = prefix + "011"
            rid = prefix + "100"
        if l in leafs and r in leafs:
            tp_l = LogicalPlan(leaf_map[l], node_id=lid)
            tp_r = LogicalPlan(leaf_map[r], node_id=rid)
            if not leaf_map[l].compatible(leaf_map[r]):
                raise Exception("Incompatible leafs")
            plan = LogicalPlan(tp_l, tp_r, operator)
            return plan
        elif l in leafs:
            tp_l = LogicalPlan(leaf_map[l], node_id=lid)
            plan = LogicalPlan(tp_l, self.lw_plan_from_tree(r, out_edges, leafs, leaf_map, prefix=rid), operator)
            return plan
        elif r in leafs:
            tp_r = LogicalPlan(leaf_map[r], node_id=rid)
            plan = LogicalPlan(tp_r, self.lw_plan_from_tree(l, out_edges, leafs, leaf_map, prefix=lid), operator)
            return plan
        else:
            plan = LogicalPlan(self.lw_plan_from_tree(l, out_edges, leafs, leaf_map, prefix="001"), self.lw_plan_from_tree(r, out_edges, leafs, leaf_map, prefix="010"), Fjoin)
            return plan


    def create_best(self, triple_patterns, query):
        start = time()
        best_plan = None
        max_delta = 0
        prev_delta = 0
        evals = 0
        best_rob = float("inf")
        done = False
        while not done:
            for i in range(100):
                max_delta = 0
                try:
                    plan = self.random_plan(triple_patterns)
                    plan.compute_cost(self.cost_model)
                    pp = PhysicalPlan(self.source, self.eddies, plan.logical_plan, query)
                    rob = pp.average_cost(self.cost_model)
                    evals +=1
                    if not best_plan:
                        best_plan = plan
                    delta = plan.cost - best_plan.cost
                    if delta > max_delta:
                        max_delta = delta
                    if delta < 0:
                        best_plan = plan
                except Exception as e:
                    pass
            print(max_delta, prev_delta)
            if max_delta - prev_delta < 0:
                break
            else:
                prev_delta = max_delta
            done = True

        print evals

        print best_plan.cost, rob
        print(time()-start)
        return pp

    def create_plan(self, query):
        triple_patterns = list(query.where.left.triple_patterns)
        for triple_pattern in triple_patterns:
            get_metadata(self.source, triple_pattern)

        return self.create_best(triple_patterns, query)

if __name__ == '__main__':

    from nlde.util.sparqlparser import parse
    #sources = "http://aifb-ls3-vm8.aifb.kit.edu:5000/dbpedia2014en"
    source = "http://aifb-ls3-vm8.aifb.kit.edu:5000/dbpedia2014en"
    query_fn = "../../example.rq"
    queryparsed = parse(open(query_fn).read())

    cost_model = CropCostModel()

    optimizer = GeneticOptimizer(eddies=2, source=source, cost_model=cost_model)
    plan = optimizer.create_plan(queryparsed)