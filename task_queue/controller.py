from nlde.engine.contact_source import get_metadata
from nlde.query.sparql_parser import parse
from nlde.engine.eddynetwork import EddyNetwork
from nlde.operators.xnjoin import Xnjoin
from nlde.operators.polyxnjoin import Poly_Xnjoin
from nlde.operators.fjoin import Fjoin
from nlde.operators.independent_operator import IndependentOperator
from nlde.operators.dependent_operator import DependentOperator
from nlde.query import BGP, TriplePattern, Argument
from crop.query_plan_optimizer.logical_plan import LogicalPlan, LogicalUnion
from crop.query_plan_optimizer.physical_plan import PhysicalPlan
from crop.query_plan_optimizer.ldff_optimizer import LDFF_Optimizer
import json, re
from time import time

import logging
logging.getLogger("nlde_logger").setLevel(logging.WARNING)

def dict_to_logical(plan_dict, sources):
    left = None
    right = None
    join = None

    for key, value in plan_dict.items():
        if key == 'right':
            right = dict_to_logical(plan_dict['right'], sources)
        if key == 'left':
            left = dict_to_logical(plan_dict['left'], sources)
        if key == 'type':
            if value == 'NLJ':
                join = Xnjoin
            else:
                join = Fjoin

        if key == 'tpf':
            pattern_var = re.compile(r'\?\w+')
            pattern_uri = re.compile(r'\<[^<^>]+\>')
            pattern_literal = re.compile(r'[\'"].*[\'"]@?\w*')

            matches_var = pattern_var.finditer(value)
            matches_uri = pattern_uri.finditer(value)
            matches_literal = pattern_literal.finditer(value)
            
            matches_var = [(m.start(), m.group(0)) for m in matches_var]
            matches_uri = [(m.start(), m.group(0)) for m in matches_uri]
            matches_literal = [(m.start(), m.group(0)) for m in matches_literal]
            
            arguments = [matches_var, matches_uri, matches_literal]

            arguments = proc_arguments(arguments)

            triple_pattern = TriplePattern(Argument(arguments[0]), Argument(arguments[1]),
                                           Argument(arguments[2]))
            cardinality = int(plan_dict.get("cardinality", 0))
            triple_pattern.cardinality = cardinality
            triple_pattern.sources = {
                sources[0]: cardinality
            }
            print('--- Now printing Triple Pattern: ---')
            print(triple_pattern)
            print('------')
            return LogicalPlan(triple_pattern)

    print plan_dict
    logical_plan = LogicalPlan(left, right, join)
    logical_plan.cardinality = int(plan_dict.get("estimated_cardinality", 0))
    return logical_plan

def proc_arguments(arguments):
    arguments_final = []
    for matches in arguments:
        for match in matches:
            arguments_final.append(match)
    arguments_final.sort(key=take_first)
    arguments_final = [el[1] for el in arguments_final]
    return arguments_final

def take_first(elem):
    return elem[0]