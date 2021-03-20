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
import sys


import logging
logger = logging.getLogger("nlde_logger")

import math
PAGE_SIZE = 100

cardinality_estimation = CardinalityEstimation()

def color_nodes(graph):
    color_map = {}
    # Consider nodes in descending degree
    for node in sorted(graph, key=lambda x: len(graph[x]), reverse=True):
        neighbor_colors = set(color_map.get(neigh) for neigh in graph[node])
        color_map[node] = next(
            color for color in range(len(graph)) if color not in neighbor_colors
        )
    return color_map

def get_tps(triples):

    if isinstance(triples, list) and isinstance(triples[0], TriplePattern):
        return triples
    elif isinstance(triples, list):
        return get_tps(triples[0])
    else:
        return get_tps(triples.triples)

def capability_aware_decomp(decomposition, sparql_exclusive=True):

    D = []
    for id, subquery in decomposition.items():
        for source in subquery[1]:
            if source.startswith("sparql@") and len(subquery[0]) > 1:
                count = get_metadata([source], subquery[0])
                if count > 0:
                    S_c = (BGP(subquery[0]), source, count)
                    D.append(S_c)
                else:
                    for triple_pattern in subquery[0]:
                        S_c = (triple_pattern, source, triple_pattern.sources[source])
                        D.append(S_c)
            else:
                for triple_pattern in subquery[0]:
                    if source in triple_pattern.sources.keys():
                        S_c = (triple_pattern, source, triple_pattern.sources[source])
                        D.append(S_c)

    return D


def decomposition_to_plan(decomposition):

    leafs = {}

    for subquery, source, cardinality in decomposition:

        if isinstance(subquery, TriplePattern):
            if subquery in leafs.keys():
                leafs[subquery].sources[source] = cardinality
                leafs[subquery].cardinality += cardinality
            else:
                new_triple_pattern = TriplePattern(subquery[0], subquery[1], subquery[2], sources={source : cardinality})
                new_triple_pattern.cardinality = cardinality
                leafs[subquery] = new_triple_pattern
        elif isinstance(subquery, BGP):
            if subquery in leafs.keys():
                for tp in leafs[subquery]:
                    for bgp_tp in subquery:
                        if tp == bgp_tp:
                            tp.sources[source] = bgp_tp.cardinality
                leafs[subquery].cardinality += cardinality

            else:
                new_tps = []
                for triple_pattern in subquery:
                    new_triple_pattern = TriplePattern(triple_pattern[0], triple_pattern[1], triple_pattern[2], sources={source : cardinality})
                    new_tps.append(new_triple_pattern)
                new_bgp = BGP(new_tps)
                new_bgp.cardinality = cardinality
                leafs[subquery] = new_bgp

    access_plans = []

    for tp in leafs.values():
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



if __name__ == '__main__':

    datasets = ["chebi", "kegg", "db", "jamendo", "nytimes", "lmdb", "swdf", "geonames"]
    sources = [ "http://aifb-ls3-vm8.aifb.kit.edu:5000/{}".format(ds) for ds in datasets ]
    sources.append("sparql@http://aifb-ls3-merope.aifb.kit.edu:8891/sparql")
    queryname =  "LD9" # str(sys.argv[1])
    q_fn = "queries/fedbench/{}.rq".format(queryname)
    #query_str = open(q_fn).read()
    query_str = open("/Users/larsheling/Documents/Development/crop.nosync/queries/fedbench/LD9.rq").read()
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

    tp_sources = set()
    graph = {}
    for i in range(index):
        for j in range(i,index):
            if i != j:
                a = id2tp[i]
                b = id2tp[j]

                a_just_tp = True if False in [s_i.startswith("sparql@") for s_i in a.sources.keys()] else False
                b_just_tp = True if False in [s_i.startswith("sparql@") for s_i in b.sources.keys()] else False

                if len(set(a.variables).intersection(set(b.variables))) == 0 or \
                        len(set(a.sources.keys()).intersection(set(b.sources.keys()))) == 0 or \
                        len(set(a.sources.keys()).intersection(tp_sources)) > 0 or \
                        len(set(b.sources.keys()).intersection(tp_sources)) > 0 :

                    graph.setdefault(i, []).append(j)
                    graph.setdefault(j, []).append(i)

                elif a_just_tp or b_just_tp:
                # If either any source of a or any source of b only supports TP evaluation, we cannot put them
                # together in the decompostion
                    graph.setdefault(i, []).append(j)
                    graph.setdefault(j, []).append(i)

                else:
                    graph.setdefault(i, [])
                    graph.setdefault(j, [])

    colors = color_nodes(graph)
    subqueries = {}
    for id, tp in id2tp.items():
        if not colors[id] in subqueries.keys():
            subqueries[colors[id]] = ([], set())
        subqueries[colors[id]][0].append(tp)
        subqueries[colors[id]][1].update(tp.sources.keys())

    D = capability_aware_decomp(subqueries)
    lplan = decomposition_to_plan(D)
    print lplan

    plan = PhysicalPlan(sources, 2, lplan, poly_operator=True)

    En = EddyNetwork()
    result_count = 0
    for result in En.execute_standalone(plan):
        #print result
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

