import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from time import time

import logging
import datetime
from nlde.util.sparqlresult_parser import parse_response
from nlde.query import BGP
from nlde.operators.operatorstructures import EOF

# Setting up Requests with a retry strategy
retry_strategy = Retry(
    total=3,
    status_forcelist=[404, 429, 500, 502, 503, 504],
    method_whitelist=["GET"],
    backoff_factor=1
)
adapter = HTTPAdapter(max_retries=retry_strategy)
http = requests.Session()
http.mount("https://", adapter)
http.mount("http://", adapter)

PAGE_SIZE = 500

accept = "application/sparql-results+json,application/json"

# Prepare parameters of request.
headers = {"Accept": accept,
           "user-agent": "MyTPFClient",
#           "Cache-Control": "no-cache",
#           "Pragma": "no-cache"
           }

count_tmplt = "SELECT (COUNT(*) as ?cnt) WHERE {{ {} }}"
select_tmplt = "SELECT {} WHERE {{ {} }}"
values_tmplt = "SELECT {} WHERE {{ {} VALUES {} {{ {} }} }}"

ESCAPE_CHARS = {
    '"' : '\\"'
}


def get_metadata_endpoint(servers, query):

    logger = logging.getLogger("nlde_logger")
    logger_debug = logging.getLogger("nlde_debug")
    sum = 0
    for server in servers:

        # Prepare parameters and header of request
        if isinstance(query, list) or isinstance(query, BGP):
            query_list = query
        else:
            query_list = [query]

        query_str = "\n".join([str(tp) for tp in query_list])
        count_query = count_tmplt.format(str(query_str))

        params = {
            "query": count_query,
            "format": "json",
            "timeout": "30000"
        }

        response = http.get(server, params=params, headers=headers)
        total = -1

        # Successfully contacted the server.
        if response.status_code == 200:
            try:
                res = response.json()
                if len(res['results']['bindings']) == 0:
                    total = 0
                else:
                    total = int(res['results']['bindings'][0]['cnt']['value'])
                sum += total
                if total > 0:
                    for tp in query_list:
                        server_name = "sparql@{}".format(server)
                        tp.sources[server_name] = total
            except Exception as e:
                raise e
        else:
            print "Could not contact SPARQL endpoint {}. Status code: {}".format(server,response.status_code)
            print response.url

        if logger:
            mgs = {
                "requests" : 1,
                "querypath" : response.url,
                "timestamp" : str(datetime.datetime.now()),
                "count" : total,
                "interface": "sparql"
            }
            logger.info(str(mgs))
            mgs = {
                "querypath": response.url,
            }
            logger_debug.info(str(mgs))

    if not isinstance(query, list):
        query.cardinality = sum
    return sum


def contact_single_endpoint_server(server, query, queue, vars=None, **kwargs):

    logger = logging.getLogger("nlde_logger")
    logger_debug = logging.getLogger("nlde_debug")

    projection_vars = set()
    if not isinstance(query, list):
        query = [query]

    query_str = "\n".join([str(tp) for tp in query])
    for tp in query:
        projection_vars.update(tp.variables)
    base_query = select_tmplt.format(" ".join(["?{}".format(var) for var in projection_vars]), query_str)

    # Pagination settings.
    limit = kwargs.get("limit", PAGE_SIZE)
    offset = 0
    total = 0
    count_requests = 0
    card_sum = 0
    elapsed = 0
    while True:
        count_requests = count_requests + 1
        if limit > 0:
            select_query = "{} LIMIT {} OFFSET {}".format(base_query, limit, offset*limit)
        else:
            select_query = base_query

        params = {
            "query": select_query,
            "format": "json",
            "timeout": "30000"
        }
        # Successfully contacted the server.
        response = http.get(server, params=params, headers=headers)

        elapsed += response.elapsed.total_seconds()
        total = -1

        # Successfully contacted the server.
        if response.status_code == 200:
            res = response.json()
            results = 0
            if len(res['results']['bindings']) == 0:
                break
            else:
                total += len(res['results']['bindings'])
                results = parse_response(queue, res['results']['bindings'])
                card_sum += results

            if limit == 0 or results < limit:
                break

            if limit == 0:
                break

            offset += 1

        else:
            print "Could not contact SPARQL endpoint {}. Status code: {}".format(server,response.status_code)
            print response.url
            #print response.headers
            break

    if logger:
        mgs = {
            "requests" : count_requests,
            "tuples" : card_sum,
            "timestamp" : str(datetime.datetime.now()),
            "elapsed" : elapsed,
            "interface": "sparql"
        }
        logger.info(str(mgs))
        mgs = {
            "querypath": response.url
        }
        logger_debug.info(str(mgs))


    if kwargs.get("put_eof", False):
        eof = EOF(requests=count_requests)
        queue.put(eof)

def contact_single_endpoint_server_bindings(server, query, queue, bindings, vars=None, **kwargs):

    logger = logging.getLogger("nlde_logger")
    logger_debug = logging.getLogger("nlde_debug")

    if len(bindings) == 0:
        print "done"
        queue.put("EOF")

    if not isinstance(query, list):
        query = [query]


    vars = " ".join(["?{}".format(var) for var in  bindings[0].keys()])
    query_str = "\n".join([str(tp) for tp in query])

    bindings_str = ""
    bound_values = set()
    for binding in bindings:
        inner_str = ""
        for var, value in binding.items():
            if value.startswith("http://") or value.startswith("https://"):
                inner_str += "<{}> ".format(value)
            else:
                # Escape Characters
                for to_replace, replace_with in ESCAPE_CHARS.items():
                    value = value.replace(to_replace, replace_with)
                inner_str += '"{}" '.format(value)

        if not inner_str in bound_values:
            bindings_str += "( {} )\n".format(inner_str)
        bound_values.add(inner_str)

    projection_vars = set()
    for tp in query:
        projection_vars.update(tp.variables)
    base_query = values_tmplt.format(" ".join(["?{}".format(var) for var in projection_vars]), query_str, "( {} )".format(vars),bindings_str)

    # Pagination settings.
    limit = kwargs.get("limit", PAGE_SIZE)
    offset = 0
    count_requests = 0
    card_sum = 0
    elapsed = 0
    while True:
        count_requests = count_requests + 1
        if limit > 0:
            values_query = "{} LIMIT {} OFFSET {}".format(base_query, limit, offset * limit)
        else:
            values_query = base_query

        #print values_query
        params = {
            "query": values_query,
            "format": "json",
            "timeout": "30000"
        }
        # Successfully contacted the server.
        response = http.get(server, params=params, headers=headers)
        elapsed += response.elapsed.total_seconds()
        total = -1

        # Successfully contacted the server.
        if response.status_code == 200:
            res = response.json()
            if len(res['results']['bindings']) == 0:
                break
            else:
                total += len(res['results']['bindings'])
                results = parse_response(queue, res['results']['bindings'])
                card_sum += results

            if limit == 0 or results < limit:
                break

            offset += 1

        else:
            print "Could not contact SPARQL endpoint {}. Status code: {}".format(server,response.status_code)
            print response.url
            break

    if logger:
        mgs = {
            "requests" : count_requests,
            "tuples" : card_sum,
            "timestamp" : str(datetime.datetime.now()),
            "elapsed" : elapsed,
            "interface": "sparql"
        }
        logger.info(str(mgs))
        mgs = {
            "querypath": response.url
        }
        logger_debug.info(str(mgs))

    return count_requests


if __name__ == '__main__':

    from nlde.query import TriplePattern
    from nlde.query import Argument
    from multiprocessing import Queue

    queue = Queue()
    servers = ["http://aifb-ls3-merope.aifb.kit.edu:8891/sparql"]
    tp1 = TriplePattern(Argument("?x"), Argument("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"),
                          Argument("<http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/drugs>"))
    tp2 = TriplePattern(Argument("?x"), Argument("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"),
                          Argument("?y"))


    instances = [{
        "x" : "http://dbpedia.org/resource/Calonectris"
    }]
    res = contact_single_endpoint_server_bindings(servers[0], [tp2], instances, queue,None)

    tuple = queue.get(True)
    while tuple != "EOF":
        print tuple
        tuple = queue.get()