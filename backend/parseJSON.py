from nlde.engine.contact_source import get_metadata
from nlde.query.sparql_parser import parse
from nlde.engine.eddynetwork import EddyNetwork
from nlde.operators.xnjoin import Xnjoin
from nlde.operators.fjoin import Fjoin
from nlde.query import BGP, TriplePattern, Argument
from crop.query_plan_optimizer.logical_plan import LogicalPlan, LogicalUnion
from crop.query_plan_optimizer.physical_plan import PhysicalPlan

import json

json_string = '''{"right": {"tpf": "?v3 <http://db.uwaterloo.ca/~galuc/wsdbm/hasGenre> ?v0 .", "cardinality": "58735", "type": "Leaf"}
, "type": "NLJ"
, "left": {"right": {"tpf": "?v3 http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://db.uwaterloo.ca/~galuc/wsdbm/ProductCategory2> .", "cardinality": "1640", "type": "Leaf"}
            , "type": "NLJ"
            , "left": {"tpf": "?v3 <http://schema.org/trailer> ?v5 .", "cardinality": "307", "type": "Leaf"}}}'''

plan_dict = json.loads(json_string)

def dict_to_logical(plan_dict):
    left = None
    right = None
    join = None
    
    for key, value in plan_dict.items():

        if key == 'right':
            right = dict_to_logical(plan_dict['right'])
        if key == 'left':
            left = dict_to_logical(plan_dict['left'])
        if key == 'type':
            if value == 'NLJ':
                join = Xnjoin
            else:
                join = Fjoin

        if key == 'tpf':
            arguments = value.split(" ")[:-1]
            # triple_pattern = " ".join(triples)
            triple_pattern = TriplePattern(Argument(arguments[0]), Argument(arguments[1]),
                         Argument(arguments[2]))
            return LogicalPlan(triple_pattern)
        
    return LogicalPlan(left, right, join)

sources = ["tpf@http://aifb-ls3-vm8.aifb.kit.edu:5000/watdiv"]

lplan = dict_to_logical(plan_dict)
plan1 = PhysicalPlan(sources, 2, lplan, poly_operator=False)

from custom_plan import custom_plan
plan2 = custom_plan(sources)

print 'Are they equal: ' + str(plan1 == plan2)