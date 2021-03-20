from nlde.query.triple_pattern import TriplePattern
from nlde.engine.contact_source import get_metadata
from nlde.query.sparql_parser import parse as parse_new
from nlde.engine.eddynetwork import EddyNetwork
from nlde.operators.xnjoin import Xnjoin
from nlde.operators.fjoin import Fjoin
from crop.costmodel.cardinality_estimation import CardinalityEstimation
from nlde.query import BGP
from crop.query_plan_optimizer.logical_plan import LogicalPlan
from crop.query_plan_optimizer.physical_plan import PhysicalPlan
from ast import literal_eval
from itertools import combinations

import logging
logger = logging.getLogger("nlde_logger")

import math
PAGE_SIZE = 100

cardinality_estimation = CardinalityEstimation()

def get_tps(triples):

    if isinstance(triples, list) and isinstance(triples[0], TriplePattern):
        return triples
    elif isinstance(triples, list):
        return get_tps(triples[0])
    else:
        return get_tps(triples.triples)

def decomposition_to_plan(decomposition):

    access_plans = []
    for tp in decomposition:
        access_plans.append(LogicalPlan(tp))

    todo = sorted(access_plans, key=lambda x: x.cardinality)
    plan = todo[0]
    todo.remove(plan)

    while len(todo):
        for i in range(len(todo)):
            if len(plan.variables.intersection(todo[i].variables)) > 0:
                plan = LogicalPlan(plan, todo[i], get_physical_operator(plan, todo[i]))
                plan.compute_cardinality(cardinality_estimation)
                todo.remove(todo[i])
                break
        else:
            # In case we cannot find another join able triple pattern
            next_tp = todo[0]
            plan = LogicalPlan(plan, next_tp, get_physical_operator(plan, next_tp))
            plan.compute_cardinality(cardinality_estimation)
            todo.remove(next_tp)
    return plan

def get_physical_operator(left, right):

    # TODO: In case we have poly joins, we need to adjust the request number estimation

    left_cost = 0
    if left.is_triple_pattern:
        left_cost = math.ceil(left.cardinality / PAGE_SIZE)

    nlj_requests = left_cost + left.cardinality * len(right.source_set)
    hj_requests = left_cost + math.ceil(right.cardinality / PAGE_SIZE)

    #if right_plan.is_triple_pattern and right_plan.left_plan.variable_position == 5:
    #    if right_plan.left_plan[1].value == "<http://www.w3.org/2002/07/owl#sameAs>":
    #        return Xnjoin

    if nlj_requests < hj_requests:
        #if not left_plan.is_triple_pattern and not right_plan.is_triple_pattern:
        #    return Fjoin
        return Xnjoin
    else:
        return Fjoin

def merge_subqueries(triple_patterns):

    Q = triple_patterns
    change = True
    while change:

        for sq_i, sq_j in combinations(Q,2):

            if sq_i.compatible(sq_j):
                if len(sq_i.sources.keys()) == 1 and len(sq_j.sources.keys()) == 1 \
                    and len(set(sq_i.sources.keys()).intersection(set(sq_j.sources.keys()))) == 1:
                        if sq_i.sources.keys()[0].startswith("sparql@"):
                            Q.remove(sq_j)
                            Q.remove(sq_i)

                            if isinstance(sq_i, BGP):
                                tps = sq_i.triple_patterns
                            else:
                                tps = [sq_i]

                            if isinstance(sq_j, BGP):
                                tps.extend(sq_j.triple_patterns)
                            else:
                                tps.append(sq_j)

                            new_sq = BGP(tps)
                            Q.append(new_sq)
                            break
        else:
            change = False

    return Q

if __name__ == '__main__':

    from pprint import pprint
    datasets = ["chebi", "kegg", "db", "jamendo", "nytimes", "lmdb", "swdf", "geonames"]
    sources = [ "http://aifb-ls3-vm8.aifb.kit.edu:5000/{}".format(ds) for ds in datasets ]
    sources.append("sparql@http://aifb-ls3-merope.aifb.kit.edu:8891/sparql")

    sources = ["sparql@https://query.wikidata.org/sparql", "sparql@http://dbpedia.org/sparql/"]
    sources = ["https://query.wikidata.org/bigdata/ldf", "sparql@http://dbpedia.org/sparql/"]
    sources = ["sparql@https://query.wikidata.org/sparql", "http://fragments.dbpedia.org/2016-04/en"]
    #sources = ["https://query.wikidata.org/bigdata/ldf", "http://fragments.dbpedia.org/2016-04/en"]

    queryname =  "LD3" # str(sys.argv[1])
    q_fn = "queries/fedbench/{}.rq".format(queryname)
    #query_str = open(q_fn).read()
    query_str = open("/Users/larsheling/Documents/Development/crop.nosync/queries/fedbench/{}.rq".format(queryname)).read()
    query_str = open("../../queries/federated/fq_example2.rq").read()
    query_parsed = parse_new(query_str)

    fhandler = logging.FileHandler('../../logs/{}.log'.format(queryname), 'w')
    fhandler.setLevel(logging.INFO)
    logger.addHandler(fhandler)

    tps = get_tps(query_parsed.body.triples)

    id2tp = {}
    index = 0
    for tp in tps:
        if isinstance(tp, TriplePattern):
            get_metadata(sources, tp)
            id2tp[index] = tp
            index += 1
    print("Got Metadata")

    D = merge_subqueries(tps)
    lplan = decomposition_to_plan(D)
    print lplan

    plan = PhysicalPlan(sources, 2, lplan, poly_operator=True)

    En = EddyNetwork()
    result_count = 0
    for result in En.execute_standalone(plan):
        # print result
        result_count += 1

    request = 0
    with open('../../logs/{}.log'.format(queryname), "r") as infile:
        try:
            for line in infile.readlines():
                if "request" in line:
                    dct = literal_eval(line)
                    request += dct['requests']
        except Exception as e:
            print line
            print e
    print plan
    print "Requests: {}, Results: {}".format(request, result_count)