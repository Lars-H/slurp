from celery import Celery
from celery.utils.log import get_task_logger
from controller import *
from pymongo import MongoClient
import os
import signal
import json
from nlde.query.sparql_parser import parse
import json
import hashlib
from collections import MutableMapping
from time import time

# logger = get_task_logger(__name__)
logger = logging.getLogger("nlde_logger")
logger.setLevel(logging.INFO)

app = Celery('tasks', broker='redis://redis:6379/0',
             backend='redis://redis:6379/0')


def get_query_hash(query):
    # TODO: Calculate isomorph queries
    query_hash = hashlib.sha256(str(query).encode('utf-8')).hexdigest()
    return query_hash


def calc_query_plan_hash(plan):
    relevant_keys = ["type", "left", "right", "cardinality", "tpf"]
    filtered_plan = filter_dict_keys(plan, relevant_keys)
    plan_hash = hashlib.sha256(json.dumps(
        filtered_plan).encode('utf-8')).hexdigest()
    return plan_hash


# keys: Remove all keys in the dictionary except the ones contained in keys
# Removes a new dict (is not in place)
def filter_dict_keys(dictionary, keys):
    keys_set = set(keys)

    modified_dict = {}
    for key, value in dictionary.items():
        if key in keys_set:
            if isinstance(value, MutableMapping):
                modified_dict[key] = filter_dict_keys(value, keys_set)
            else:
                modified_dict[key] = value
    return modified_dict


@app.task()
def execute_plan(query_id, sources_json, plan_json, query_json, mongodb_url):

    # Results are aggregated & written to DB every X results
    write_to_db_every = 10

    # Maximum time in seconds before query is stopped
    maximum_time_per_query = 60

    task_id = query_id
    t0 = time()

    sources = sources_json
    plan_dict = json.loads(plan_json)
    print "Got Plan Dict:",  plan_dict
    # Connect to Database
    try:
        client = MongoClient(mongodb_url)
    except Exception as e:
        return json.dumps({'title': 'Could not connect to database.', 'msg': 'Authentication failed.'})

    db = client.querydb
    queries = db.queries

    queries.update_one(
        {'_id': task_id},
        {'$set': {
            'status': 'pending'}}
    )

    # Physical Plan creation error - i. e. due to manipulated plan request
    try:
        lplan = dict_to_logical(plan_dict, sources)
        parsedQuery = parse(query_json)
        plan = PhysicalPlan(sources, 2, lplan,
                            poly_operator=False, query=parsedQuery)
        variables = plan.tree.vars
        queries.update_one(
            {'_id': task_id},
            {'$set': {
                    'sparql_results.head.vars': list(variables) }}
        )

    except Exception:
        queries.update_one(
            {'_id': task_id},
            {'$set': {
                'status': 'failed'}}
        )
        return json.dumps({'title': 'Physical plan creation error', 'msg': 'Could not create a physical plan for the query execution from the plan provided in the request.'})

    print(plan)

    En = EddyNetwork()
    result_count = 0

    counter = 0
    aggregated_solutions = []
    status = 'done'
    print("Vor der Eddie Schleife")

    # Set Timout for query execution
    signal.signal(signal.SIGALRM, En.stop_execution)
    signal.alarm(maximum_time_per_query)

    try:
        t0 = time()
        for result in En.execute_standalone(plan):
            t_elasped = time() - t0
            logger.info(result)

            # Add variables to variable set (for "head" of sparql result)
            #variables.update(result.data.keys())

            solution_dict = {}
            counter += 1
            result_count += 1
            # Check if URI or Literal

            for key, value in result.data.items():
                if str(value).startswith('http://') or str(value).startswith('https://'):
                    val_type = 'uri'
                else:
                    val_type = 'Literal'
                solution_dict[key] = {'value': value, 'type': val_type}
            solution_dict['_trace_'] = {'value': str(t_elasped), 'type': 'Literal', 'count': str(result_count) }
            aggregated_solutions.append(solution_dict)



            # Write aggregated results to MongoDB every X results
            if (counter % write_to_db_every == 0):
                t_now = time()
                queries.update_one(
                    {'_id': task_id},
                    {'$push': {
                        'sparql_results.results.bindings': {'$each': aggregated_solutions}
                    },
                        '$set': {
                            'result_count': result_count,
                            't_delta': t_now - t0,
                            'sparql_results.head.vars': list(variables)}}
                )

                counter = 0
                aggregated_solutions = []
    except Exception as e:
        print(e)
        raise e

    updated_plan_dict = plan.json_dict
    query_hash = get_query_hash(parsedQuery)
    plan_hash = calc_query_plan_hash(updated_plan_dict)
    tend = time()

    if tend - t0 > maximum_time_per_query:
        status = "timeout"
    # Write remaining results to MongoDB

    queries.update_one(
        {'_id': task_id},
        {'$push': {
            'sparql_results.results.bindings': {'$each': aggregated_solutions}},
         '$set': {
            'plan': updated_plan_dict,
            'query_hash': query_hash,
            'plan_hash': plan_hash,
            'requests': plan.total_requests,
            'status': status,
            't_end': tend,
            't_delta': (tend-t0),
            'result_count': result_count,
            'sparql_results.head.vars': list(variables)}}
    )

    return ' Count: ' + str(result_count)
