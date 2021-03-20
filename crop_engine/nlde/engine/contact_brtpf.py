"""
Created on Mar 23, 2015

@author: Maribel Acosta
@author: Lars Heling
"""
import requests
from re import findall
from nlde.util.turtle_parser import parse_response
import logging
from nlde.operators.operatorstructures import EOF

triples_regex = r"void:triples\D*(\d+)"

user_agent = "CROP"
accept = "text/turtle"
import datetime
from time import time

headers = {"Accept": accept,
           "user-agent": "MyTPFClient",
#           "Cache-Control": "no-cache",
#           "Pragma": "no-cache"
           }

def get_metadata_brtpf(servers, query):

    logger = logging.getLogger("nlde_logger")

    sum = 0
    for server in servers:

        # Prepare parameters and header of request
        params = {
            "subject": query.subject.value,
            "predicate": query.predicate.value,
            "object": query.object.value
        }


        response = requests.get(server, params=params, headers=headers)

        # Successfully contacted the server.
        if response.status_code == 200:
            res = response.content
            matches = findall(triples_regex, res)
            if len(matches) == 1:
                total = int(matches[0])
                # Return estimated number of triples in fragment.
                sum += total
                if total > 0:
                    server_name = "brtpf@{}".format(server)
                    query.sources[server_name] = total
        else:
            print "Could not contact brTPF server {}. Status code: {}".format(server, response.status_code)

        if logger:
            mgs = {
                "requests" : 1,
                "querypath" : response.url,
                "timestamp" : str(datetime.datetime.now()),
                "count" : total,
                "interface" : "brtpf"
            }
            logger.info(str(mgs))


    query.count = sum
    return sum


def contact_brtpf_server(servers, query, queue, vars=None):

    for server in servers:
        if server in query.sources.keys():
            contact_single_brtpf_server(server, query, queue, vars)
    queue.put("EOF")

def contact_single_brtpf_server(server, query, queue, vars=None, binding=[], **kwargs):
    logger = logging.getLogger("nlde_logger")

    template = 0
    qvars = []
    param_dict = {}


    ## Get the values string for the URL
    if binding and len(binding) > 0:
        values_str, bound_vars = get_values_str(binding)
        param_dict['values'] = values_str
    else:
        bound_vars = []

    # Extract subject.
    subject = query.subject
    if subject.isvariable():
        template = template | 4
        qvars.append(subject.get_variable())

        # Add variable to query string
        if subject.value in bound_vars:
            param_dict['subject'] = subject.value
    else:
        param_dict['subject'] = subject.value

    # Extract predicate.
    predicate = query.predicate
    if predicate.isvariable():
        template = template | 2
        qvars.append(predicate.get_variable())

        # Add variable to query string
        if predicate.value in bound_vars:
            param_dict['predicate'] = predicate.value
    else:
        param_dict['predicate'] = predicate.value

    # Extract object (value).
    value = query.object
    if value.isvariable():
        template = template | 1
        qvars.append(value.get_variable())

        # Add variable to query string
        if value.value in bound_vars:
            param_dict['object'] = value.value
    else:
        if value.isfloat():
            param_dict['object'] = '"{}"^^{}'.format(value, "<http://www.w3.org/2001/XMLSchema#double>")
        elif value.isint():
            param_dict['object'] = '"{}"^^{}'.format(value, "<http://www.w3.org/2001/XMLSchema#integer>")
        else:
            if not value.value.startswith("http") and not value.isuri() and value.value[0] != '"' and value.value[-1] != '"':
                param_dict['object'] = '"{}"'.format(value.value)
            else:
                param_dict['object'] = value.value

    # Literal in subject or predicate position: NOT a valid triple pattern.
    if subject.isliteral() or predicate.isliteral():
        queue.put("EOF")
        return

    # When there are no variables in the TP,
    # Get the ones provides
    if len(qvars) == 0 and vars:
        qvars = vars

    if vars:
        qvars.append(vars)

    # Pagination settings.
    page = 1
    next_page = True
    total = 0
    count_requests = 0
    card_sum = 0
    elapsed = 0
    while next_page:
        count_requests = count_requests + 1
        # Establish connection and get response from server.
        param_dict['page'] = page
        response = requests.get(server, params=param_dict, headers=headers)
        elapsed += response.elapsed.total_seconds()
        #print response.url
        next_page = False

        #print response.request.

        # Successfully contacted the server.
        if response.status_code == 200:
            res = str(response.content)
            if page == 1:
                # Get total solutions in fragment.
                matches = findall(triples_regex, res)
                if len(matches) == 1:
                    total = int(matches[0])
                else:
                    break

            # Get solution mappings from fragment.
            if total > 0:
                #def parse_response(template, answers, var, queue, server, count, context, binding={}):
                card = parse_response(template, res, qvars, queue, server, 0)
                card_sum += card

            # Prepare next request.
            if "nextPage" in res:
                next_page = True
            elif "hydra:next" in res :
                next_page = True
            page = page + 1
        else:
            print "Could not contact brTPF server {}. Status code: {}".format(server,response.status_code)
            print response.headers

    if logger:
        mgs = {
            "requests" : count_requests,
            "tuples" : card_sum,
            "querypath" : response.url,
            "timestamp" : str(datetime.datetime.now()),
            "bindings" : str(len(binding)),
            "elapsed" : elapsed,
            "interface" : "brtpf"
        }
        logger.info(str(mgs))

    if kwargs.get("put_eof", False):
        eof = EOF(requests=count_requests)
        queue.put(eof)

    return count_requests

def contact_single_brtpf_server_binding(server, triple_pattern, queue, binding, vars=None):

    count_requests = contact_single_brtpf_server(server, triple_pattern, queue, vars, binding)
    return count_requests

def get_values_str(bindings):

    vars = set()
    bindings_str = ""
    bound_values = set()
    for index, binding in enumerate(bindings):
        inner_str = ""
        bindings_vars = set()
        for var, value in binding.items():
            if value.startswith("http://") or value.startswith("https://"):
                inner_str += "<{}> ".format(value)
            else:
                inner_str += value
            bindings_vars.add("?{}".format(var))

        # Setting the reference binding variables
        if index == 0:
            vars = bindings_vars
        else:
            # Check whether all bindings use variables consistently
            if vars != bindings_vars:
                raise Exception("Inconsistent variables in Binding")


        if not inner_str in bound_values:
            bindings_str += "({}) ".format(inner_str)
        bound_values.add(inner_str)

    values_str = "{} {{{}}}".format("( {} )".format(" ".join(vars)), bindings_str)
    return values_str, list(vars)


if __name__ == '__main__':
    from nlde.query import TriplePattern, Argument
    import logging
    from multiprocessing import Queue
    logging.basicConfig(level=logging.INFO)
    servers = ["http://aifb-ls3-vm8.aifb.kit.edu:8080/dbpedia"]
    bindings = [
        {
            "s" : "http://dbpedia.org/resource/Zack_Space"
        },
        {
            "s" : "http://dbpedia.org/resource/Abraham_Gotthelf_K%C3%A4stner"
        }
    ]

    bindings = [
        {
            "o" : "http://dbpedia.org/resource/Stanford_University"
        }
    ]

    #bindings = None
    #servers = ["http://aifb-ls3-vm8.aifb.kit.edu:5000/dbpedia"]
    query = TriplePattern(Argument("http://dbpedia.org/resource/Frank_Church"), Argument("http://dbpedia.org/ontology/almaMater"), Argument("?o")) #Argument("http://www.wikidata.org/entity/Q986"))


    #card, stats = get_metadata_ldf_stats(servers, query)
    #print stats

    if True:
        vars = {
            "s" : ["?drug"],
            "p" : [],
            "o" : []
        }

        total = get_metadata_brtpf(servers, query)
        print total
        q = Queue()
        contact_single_brtpf_server_binding(servers[0], query, q, bindings)

        items = []
        elem = q.get(True)
        while True:
            print elem
            if elem != "EOF":
                items.append(elem)
            else:
                break
            elem = q.get()
        print len(items)
