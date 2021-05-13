import sys
from celery import Celery
from flask import Flask
from flask_sslify import SSLify
from flask import jsonify
from flask import request
from flask_cors import CORS
from controller import *
from reversed_proxy import Reversed_Proxy
from pymongo import MongoClient
import json
from time import time
from random import randint
import os
import requests
import uuid

import base64

logger = logging.getLogger("nlde_logger")
logger.setLevel(logging.INFO)

backend_logger = logging.getLogger("backend")
backend_logger.setLevel(logging.INFO)

simple_app = Celery('task_queue', broker='redis://redis:6379/0',
                    backend='redis://redis:6379/0')

app = Flask(__name__)
CORS(app)

sys.stdout.flush()

mongodb_user = os.environ['MONGO_ROOT_USER']
mongodb_pass = os.environ['MONGO_ROOT_PASSWORD']
mongodb_url = 'mongodb://' + mongodb_user + \
    ':' + mongodb_pass + '@mongodb:27017'


reverse_proxy_suffix = os.environ.get('API_PROXY', "/")
logger.info("Backend: Reverse Proxy defined as '{}'".format(
    reverse_proxy_suffix))

https_enabled = bool(os.environ.get('HTTPS_ENABLED', False))
if https_enabled:
    logger.info("Backend: HTTPS enabled")
    reverse_proxy_scheme = 'https'
    SSLify(app)
else:
    logger.info("Backend: HTTPS not enabled")
    reverse_proxy_scheme = 'http'

app.wsgi_app = Reversed_Proxy(
    app.wsgi_app, script_name=reverse_proxy_suffix, scheme=reverse_proxy_scheme)


@app.route('/plan', methods=['GET'])
def get_plan():
    try:
        backend_logger.info("Request from Frontend: {}".format(request.args))
        query_str = base64.b64decode(request.args.get('query')).decode('ascii')
        sources_str = base64.b64decode(
            request.args.get('sources')).decode('ascii')
        sources = sources_str.split(',')

        optimizer_dict = {}
        if "optimizer" in request.args.keys():
            optimizer_name = base64.b64decode(
                request.args.get('optimizer')).decode('ascii')
            optimizer_dict['name'] = optimizer_name

        plan = plan_from_optimizer(query_str, sources, optimizer_dict)
        # planDict = planToDict(plan)
        planDict = plan.json_dict
        backend_logger.info(planDict)
        return jsonify(planDict)

    except requests.exceptions.ConnectionError as connec_error:
        host = connec_error.message.pool.host
        backend_logger.info(connec_error)
        print {'title': 'Connection error', 'msg': 'Host ' + str(host) + ' not found.'}
        return jsonify({'title': 'Connection error', 'msg': 'Host ' + str(host) + ' not found.'}), 400

    except IndexError:
        print {'title': 'Create plan error', 'msg': 'Unfortunately we could not create an initial query plan. Please modify your query and only use supported operators.'}
        return jsonify({'title': 'Create plan error', 'msg': 'Unfortunately we could not create an initial query plan. Please modify your query and only use supported operators.'}), 400

    except Exception as err:
        print {'title': 'Error', 'msg': 'Something went horribly wrong.'}
        return jsonify({'title': 'Error', 'msg': 'Something went horribly wrong.'}), 500


@app.route('/plan', methods=['POST'])
def execute_plan():
    req_data = request.get_json()

    sources_json = req_data['sources']
    plan_json = json.dumps(req_data['plan'])
    query = req_data['query']
    query_name = req_data['query_name']

    # Check for existing jobs and do not allow new one if there is any reserved
    try:
        i = simple_app.control.inspect()
        reserved_tasks = i.reserved()
        for key in reserved_tasks.keys():
            num_reserved_tasks = len(reserved_tasks[key])
            if num_reserved_tasks > 0:
                return jsonify({"num_tasks_before": num_reserved_tasks})
    except:
        app.logger.info('No reserved task found!')

    try:
        client = MongoClient(mongodb_url)
    except Exception as e:
        return json.dumps({'title': 'Could not connect to database.', 'msg': 'Authentication failed.'}), 500

    db = client.querydb
    queries = db.queries

    t_0 = time()

    query_id = str(uuid.uuid4())

    plan_dict = json.loads(plan_json)
    backend_logger.info("Got Plan from Frontend: {}".format(plan_dict))
    # Insert Query with initial Information
    query_obj = {
        '_id': query_id,
        'query_name': query_name,
        't_start': t_0,
        't_delta': 0,
        'requests': 0,
        'status': 'queue',
        'sources': sources_json,
        'plan': plan_dict,
        'query': query,
        'query_hash': "",
        'sparql_results':
        {
            'head': {
                'vars': []
            },
            'results': {
                'bindings': []
            }
        }
    }

    queries.insert_one(query_obj)

    # Send job
    app.logger.info("Sending Query Execution")
    r = simple_app.send_task('tasks.execute_plan', kwargs={
                             'query_id': query_id, 'sources_json': sources_json, 'plan_json': plan_json, 'query_json': query, 'mongodb_url': mongodb_url})
    app.logger.info(r.backend)

    return jsonify({"task_id": query_id})


@app.route('/result')
def getresults():

    resultsToReturn = 20

    # Connect to MongoDB to get results
    try:
        client = MongoClient(mongodb_url)
    except Exception as e:
        return json.dumps({'title': 'Could not connect to database.', 'msg': 'Authentication failed.'}), 500

    db = client.querydb
    queries = db.queries

    # Find result by Task ID
    results = list(queries.find({}, {'_id': 1, 'result_count': 1, 'status': 1,
                                     't_start': 1, 't_delta': 1, 't_end': 1,
                                     'query': 1, 'query_name': 1, 'requests': 1, }).sort('t_start', -1).limit(resultsToReturn))

    # Return result as JSON
    return jsonify(results)


@app.route('/result/<task_id>')
def getresult(task_id):

    # Connect to MongoDB to get results
    try:
        client = MongoClient(mongodb_url)
    except Exception as e:
        return json.dumps({'title': 'Could not connect to database.', 'msg': 'Authentication failed.'}), 500

    db = client.querydb
    queries = db.queries

    # Find result by Task ID
    queryresult = queries.find_one({'_id': task_id})

    # Return result as JSON
    return jsonify(queryresult)


@app.route('/result/filter/<query_name>')
def get_filtered_results(query_name):

    resultsToReturn = 20

    # Connect to MongoDB to get results
    try:
        client = MongoClient(mongodb_url)
    except Exception as e:
        return json.dumps({'title': 'Could not connect to database.', 'msg': 'Authentication failed.'}), 500

    db = client.querydb
    queries = db.queries

    regex = ".*" + query_name + ".*"
    print(regex)
    # Find result by Task ID
    results = list(queries.find({"query_name": {"$regex": regex, "$options": '-i'}}, {'_id': 1, 'result_count': 1,
                                                                                      'status': 1, 't_start': 1,
                                                                                      't_delta': 1, 't_end': 1,
                                                                                      'query': 1, 'query_name': 1,
                                                                                      'requests': 1, }).sort('t_start',
                                                                                                             -1).limit(resultsToReturn))

    print(results)

    # Return result as JSON
    return jsonify(results)


'''
Retrieve all unique query plans for a query.
If there are multiple identical plans, the latest plan is returned
'''


@app.route('/executions/hash')
def get_executions_for_identical_query():
    query_hash = base64.b64decode(
        request.args.get('query_hash')).decode('ascii')
    plan_hash = base64.b64decode(request.args.get('plan_hash')).decode('ascii')

    try:
        client = MongoClient(mongodb_url)
    except Exception as e:
        return json.dumps({'title': 'Could not connect to database.', 'msg': 'Authentication failed.'}), 500

    db = client.querydb
    queries = db.queries

    # https://stackoverflow.com/questions/52566913/how-to-group-in-mongodb-and-return-all-fields-in-result
    different_plans_for_same_query = list(
        queries.aggregate([
            {
                "$match": {
                    "query_hash": query_hash,
                    "plan_hash": {
                        "$ne": plan_hash}
                }
            },
            {
                "$group": {
                    "_id": "$plan_hash",
                    "maxQuantity": {"$max": "$t_start"},
                    "doc": {"$last": "$$ROOT"}
                }
            },
            {"$replaceRoot": {"newRoot": "$doc"}}
        ]))

    return jsonify(different_plans_for_same_query)


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
