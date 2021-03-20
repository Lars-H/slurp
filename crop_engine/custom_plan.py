from nlde.engine.contact_source import get_metadata
from nlde.query.sparql_parser import parse
from nlde.engine.eddynetwork import EddyNetwork
from nlde.operators.xnjoin import Xnjoin
from nlde.operators.fjoin import Fjoin
from nlde.operators.polyxnjoin import Poly_Xnjoin
from nlde.query import BGP, TriplePattern, Argument
from crop.query_plan_optimizer.logical_plan import LogicalPlan, LogicalUnion
from crop.query_plan_optimizer.physical_plan import PhysicalPlan
from nlde.operators.independent_operator import IndependentOperator
from nlde.operators.dependent_operator import DependentOperator
from crop.query_plan_optimizer.ldff_optimizer import LDFF_Optimizer



from time import time


import logging
logging.getLogger("nlde_logger").setLevel(logging.WARNING)


def traverse_tree(node, physical_plan=None):
    # Traverse through branches recursively and create json-seriazable dict while doing so
    branch = {}

    if isinstance(node, IndependentOperator) or isinstance(node, DependentOperator):
        branch['type'] = 'Leaf'
        branch['tpf'] = str(node.query)
        branch['cardinality'] = str(node.cardinality)
        # print node.query
    else:
        if isinstance(node.operator, Xnjoin) or isinstance(node.operator, Poly_Xnjoin):

            if physical_plan:
                op_id = node.operator.id_operator
                branch['tuples'] = physical_plan.operator_id2logical_plan[op_id].true_cardinality

            branch['type'] = 'NLJ'
            if node.right is not None:
                branch['right'] = traverse_tree(node.right, physical_plan)
            if node.left is not None:
                branch['left'] = traverse_tree(node.left, physical_plan)
        elif isinstance(node.operator, Fjoin):
            branch['type'] = 'SHJ'
            if node.right is not None:
                branch['right'] = traverse_tree(node.right, physical_plan)
            if node.left is not None:
                branch['left'] = traverse_tree(node.left, physical_plan)
        else:
            return traverse_tree(node.left, physical_plan)

    return branch


def plan_from_optimizer(query_str, sources):

    optimizer = LDFF_Optimizer(sources=sources, eddies=2, pbj=False, decomposer=False, pruning=False)
    query_parsed = parse(query_str)

    plan = optimizer.create_plan(query_parsed)
    return plan


def custom_plan(sources):

    tp_1 = TriplePattern(Argument("?v3"), Argument("<http://schema.org/trailer>"),
                         Argument("?v5"))
    tp_2 = TriplePattern(Argument("?v3"), Argument("http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"),
                         Argument("<http://db.uwaterloo.ca/~galuc/wsdbm/ProductCategory2>"))
    tp_3 = TriplePattern(Argument("?v3"), Argument("<http://db.uwaterloo.ca/~galuc/wsdbm/hasGenre>"),
                         Argument("?v0"))
    tps = [tp_1, tp_2, tp_3]

    # XNJoin = Nested Loop Join
    # FJoin: Hash Join
    l_plan = LogicalPlan(LogicalPlan(LogicalPlan(tp_1), LogicalPlan(tp_2), operator=Xnjoin), LogicalPlan(tp_3),
                         operator=Xnjoin)

    plan = PhysicalPlan(sources, 2, l_plan, poly_operator=False)
    return plan

if __name__ == '__main__':

    # SPARQL Query String
    query_str_limit = """
    SELECT DISTINCT * WHERE { 
        ?v0 <http://ogp.me/ns#tag> <http://db.uwaterloo.ca/~galuc/wsdbm/Topic75> .
        ?v0 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?v2 .
        ?v3 <http://schema.org/trailer> ?v4 .
        ?v3 <http://schema.org/keywords> ?v5 .
        ?v3 <http://db.uwaterloo.ca/~galuc/wsdbm/hasGenre> ?v0 .
        ?v3 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://db.uwaterloo.ca/~galuc/wsdbm/ProductCategory2> .
    } 
    """

    query_str_wo_limit = """
    SELECT DISTINCT * WHERE { 
        ?v0 <http://ogp.me/ns#tag> <http://db.uwaterloo.ca/~galuc/wsdbm/Topic75> .
        ?v0 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?v2 .
        ?v3 <http://schema.org/trailer> ?v4 .
        ?v3 <http://schema.org/keywords> ?v5 .
        ?v3 <http://db.uwaterloo.ca/~galuc/wsdbm/hasGenre> ?v0 .
        ?v3 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://db.uwaterloo.ca/~galuc/wsdbm/ProductCategory2> .
    }
    """

    # Define the source
    sources = ["tpf@http://aifb-ls3-vm8.aifb.kit.edu:5000/watdiv"]

    # Plan from an optimizer
    plan_limit = plan_from_optimizer(query_str_limit, sources)
    plan_wo_limit = plan_from_optimizer(query_str_wo_limit, sources)
    
    print 'Print plan operators with LIMIT:'
    print plan_limit.operators

    print 'Print plan operators w/o LIMIT:'
    print plan_wo_limit.operators
    
    print '--- Are the objects equal for w/ and w\ LIMIT: ---'
    print plan_limit == plan_wo_limit
    print '--- ---'

    # Custom Plan
    # plan = custom_plan(sources)

    # print plan

    En = EddyNetwork()
    result_count = 0
    t0 = time()
    print plan_limit
    for result in En.execute_standalone(plan_limit):
        # print result
        print result
        result_count += 1

    tdelta = time() - t0
    print tdelta, result_count
    print traverse_tree(plan_limit.tree, plan_limit)
