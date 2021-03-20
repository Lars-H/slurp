"""
Created on Mar 23, 2015

@author: Maribel Acosta
@author: Lars Heling
"""
import requests
from re import findall
from nlde.util.jsonld_parser import parse_response
from nlde.util.statsldfparser import get_authorities
import logging
from nlde.query import TriplePattern, Argument
from time import time
import urllib
from nlde.operators.operatorstructures import EOF

triples_regex = r"void:triples\D*(\d+)"

user_agent = "CROP"
accept = "application/json,application/ld+json"
import datetime

headers = {"Accept": accept,
           "user-agent": "MyTPFClient",
#           "Cache-Control": "no-cache",
#           "Pragma": "no-cache"
           }

def get_metadata_tpf(servers, query):

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
        total = -1

        # Successfully contacted the server.
        if response.status_code == 200:
            res = response.content
            matches = findall(triples_regex, res)
            if len(matches) == 1:
                total = int(matches[0])
                # Return estimated number of triples in fragment.
                sum += total
                if total > 0:
                    server_name = "tpf@{}".format(server)
                    query.sources[server_name] = total
        else:
            print "Could not contact TPF server {}. Status code: {}".format(server, response.status_code)


        if logger:
            mgs = {
                "requests" : 1,
                "querypath" : response.url,
                "timestamp" : str(datetime.datetime.now()),
                "count" : total,
                "interface": "tpf"
            }
            logger.info(str(mgs))


    query.count = sum
    return sum


def contact_tpf_server(servers, query, queue, vars=None):

    for server in servers:
        if server in query.sources.keys():
            contact_single_tpf_server(server, query, queue, vars)
    queue.put("EOF")

def contact_single_tpf_server(server, query, queue, vars=None, binding={}, **kwargs):
    logger = logging.getLogger("nlde_logger")

    template = 0
    qvars = []
    param_dict = {}

    # Extract subject.
    subject = query.subject
    if subject.isvariable():
        template = template | 4
        qvars.append(subject.get_variable())
    else:
        param_dict['subject'] = subject.value

    # Extract predicate.
    predicate = query.predicate
    if predicate.isvariable():
        template = template | 2
        qvars.append(predicate.get_variable())
    else:
        param_dict['predicate'] = predicate.value

    # Extract object (value).
    value = query.object
    if value.isvariable():
        template = template | 1
        qvars.append(value.get_variable())
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

    #if vars:
    #    qvars.append(vars)


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

        # Successfully contacted the server.
        if response.status_code == 200:
            res = response.content
            if page == 1:
                # Get total solutions in fragment.
                matches = findall(triples_regex, res)
                if len(matches) == 1:
                    total = int(matches[0])
                else:
                    break

            # Get solution mappings from fragment.
            if total > 0:
                myres = eval(res)
                card = parse_response(template, myres["@graph"], qvars, queue, server, 0, myres["@context"], binding)
                card_sum += card

            # Prepare next request.
            if "nextPage" in res:
                next_page = True
            elif "hydra:next" in res :
                next_page = True
            page = page + 1
        else:
            print "Could not contact TPF server {}. Status code: {}".format(server, response.status_code)
            print response.headers


    if logger:
        mgs = {
            "requests" : count_requests,
            "tuples" : card_sum,
            "querypath" : response.url,
            "timestamp" : str(datetime.datetime.now()),
            "elapsed" : elapsed,
            "interface": "tpf"
        }
        logger.info(str(mgs))

    if kwargs.get("put_eof", False):
        eof = EOF(requests=count_requests)
        queue.put(eof)

    return count_requests


def contact_single_tpf_server_binding(server, triple_pattern, queue, binding, vars=None):

    variables = vars # list(query.variables)

    query = [triple_pattern.subject.value, triple_pattern.predicate.value, triple_pattern.object.value]

    if len(binding) > 1:
        raise Exception("Too many bindings for TPF interface")

    binding = binding[0]

    # Instantiate variables in the query.
    inst = {}
    for i in variables:
        inst.update({i: binding[i]})
        inst_aux = str(binding[i])
        if inst_aux.startswith("http://"):
            inst_aux = "<{}>".format(inst_aux)
        for j in (0, 1, 2):
            if query[j] == "?" + i:
                query[j] = inst_aux

    tp = TriplePattern(Argument(query[0]), Argument(query[1]), Argument(query[2]))
    tp.sources = triple_pattern.sources
    vars = None
    if tp.variable_position == 0:
        #vars = list(triple_pattern.get_variables())
        vars = triple_pattern.variables_dict

    count_requests = contact_single_tpf_server(server, tp, queue, vars, binding)
    return count_requests

if __name__ == '__main__':
    from nlde.query import TriplePattern, Argument
    import logging
    from multiprocessing import Queue
    logging.basicConfig(level=logging.INFO)
    servers = ["http://fragments.dbpedia.org/2015-10/en", "https://query.wikidata.org/bigdata/ldf"]
    servers = ["http://aifb-ls3-vm8.aifb.kit.edu:5000/db"]
    #servers = ["http://aifb-ls3-remus.aifb.kit.edu:5000/dbpedia2014", "http://aifb-ls3-remus.aifb.kit.edu:5000/dbpedia2014"] #http://aifb-ls3-remus.aifb.kit.edu:5000/dblp"]
    query = TriplePattern(Argument("http://dbpedia.org/resource/Aaron_Altaras"), Argument("?p"), Argument("http://wikidata.dbpedia.org/resource/Q301638")) #Argument("http://www.wikidata.org/entity/Q986"))
    #query = TriplePattern(Argument("?x"),
    #                      Argument("http://www.w3.org/2002/07/owl#sameAs"), Argument("http://www.wikidata.org/entity/Q986"))
    #query = TriplePattern(Argument("?x"),
    #                      Argument("http://www.w3.org/2000/01/rdf-schema#label"), Argument("?y"))

    #query = TriplePattern(Argument("?s"), Argument("http://www.wikidata.org/prop/direct/P31"), Argument("http://www.wikidata.org/entity/Q6256"))
    #query = TriplePattern(Argument("http://www.wikidata.org/entity/Q31835482"), Argument("http://www.w3.org/2002/07/owl#sameAs"),
    #                      Argument("?o"))
    query = TriplePattern(Argument("<http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugs/DB00201>"), Argument("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"),
                          Argument("<http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/drugs>"))


    #card, stats = get_metadata_ldf_stats(servers, query)
    #print stats

    if True:
        vars = {
            "s" : ["?drug"],
            "p" : [],
            "o" : []
        }

        total = get_metadata_tpf(servers, query)
        q = Queue()
        contact_tpf_server(servers, query, q, vars)

        items = []
        while not q.empty():
            elem = q.get()
            items.append(elem)
            print elem
        print len(items)
