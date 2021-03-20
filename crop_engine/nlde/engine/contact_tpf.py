"""
Created on Mar 23, 2015

@author: Maribel Acosta
"""
from nlde.util.ldfparser import LDFParser
import httplib
import urllib
import logging
from nlde.util.logging_utils import RequestCountHandler


user_agent = "nLDE 0.2"
accept = "application/json"
import datetime


def get_metadata_ldf(server, query):
    logger = logging.getLogger("nlde_logger")

    # Extract server information.
    referer = server
    server = server.split("http://")[1]
    (server, path) = server.split("/", 1)
    host_port = server.split(":")
    port = 80 if len(host_port) == 1 else host_port[1]

    # Prepare parameters and header of request
    params = urllib.urlencode({"subject": query.subject.value,
                               "predicate": query.predicate.value,
                               "object": query.object.value})


    # Establish connection and get response from server.
    conn = httplib.HTTPConnection(server)
    # conn.set_debuglevel(1)
    conn.request("GET", "/" + path + "?" + params, None, headers)
    response = conn.getresponse()
    total = -1

    # Successfully contacted the server.
    if response.status == httplib.OK:
        res = response.read()
        pos1 = res.find('"void:triples"')
        pos2 = res[pos1:].find(",")
        aux = res[pos1 + len('"void:triples"'):pos1 + pos2]
        aux = aux.strip(" ").strip(":").strip(" ")
        total = int(aux)

    else:
        # TODO: Inform the user that the server could not be contacted.
        pass

        logging.info('{requests: %d, tuples : %d}', 1, total)

    if logger:
        #msg = '{"requests": {}, "tuples" : {}, "query" : {}, "timestamp" : {} }'.format(count_requests, card_sum, path + params, str(datetime.datetime.now()))
        mgs = {
            "requests" : 1,
            "querypath" : path + params,
            "timestamp" : str(datetime.datetime.now())
        }
        logger.info(str(mgs))
    # Return estimated number of triples in fragment.
    query.count = total
    return total


def contact_ldf_server(server, query, queue):
    logger = logging.getLogger("nlde_logger")

    # Extract server information.
    referer = server
    server = server.split("http://")[1]
    (server, path) = server.split("/", 1)
    host_port = server.split(":")
    port = 80 if len(host_port) == 1 else host_port[1]

    template = 0
    qvars = []
    param_dict = {}
    #{"subject": subject.value, "predicate": predicate.value, "object": value.value}
    # Build query for request:
    # extract subject, predicate, object (value).

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
            param_dict['object'] = value.value

    # Literal in subject or predicate position: NOT a valid triple pattern.
    if subject.isliteral() or predicate.isliteral():
        queue.put("EOF")
        return


    # Prepare parameters of request.
    params = "?" + urllib.urlencode(param_dict)
    headers = {"User-Agent": user_agent,
               "Referer": referer,
               "Host": server,
               "Accept": accept}

    # Pagination settings.
    page = 0
    next_page = True
    total = 0
    count_requests = 0
    card_sum = 0
    #print(query, template)
    while next_page:

        # Establish connection and get response from server.
        conn = httplib.HTTPConnection(server)
        # conn.set_debuglevel(1)
        conn.request("GET", "/" + path + params, None, headers)
        response = conn.getresponse()
        next_page = False
        count_requests = count_requests + 1
        # Successfully contacted the server.
        if response.status == httplib.OK:

            res = response.read()
            page = page + 1
            if page == 1:
                # Get total solutions in fragment.
                pos1 = res.find('"void:triples"')
                pos2 = res[pos1:].find(",")
                aux = res[pos1 + len('"void:triples"'):pos1 + pos2]
                aux = aux.strip(" ").strip(":").strip(" ")
                total = int(aux)

            # Get solution mappings from fragment.
            if total > 0:
                myres = eval(res)
                card = LDFParser(template, myres["@graph"], qvars, queue, server, 0, myres["@context"])
                card_sum += card

            # Prepare next request.
            if "nextPage" in res:
                pos1 = res.find('nextPage"')
                pos2 = res[pos1:].find(",")
                aux = res[pos1 + len('nextPage"'):pos1 + pos2]
                aux = aux.strip(" ").strip(":").strip(" ")
                params = aux.strip('"')
                next_page = True
            elif "hydra:next" in res:
                pos1 = res.find('hydra:next"')
                pos2 = res[pos1:].find(",")
                aux = res[pos1 + len('hydra:next"'):pos1 + pos2]
                aux = aux.strip(" ").strip(":").strip(" ")
                params = eval(aux.strip('"'))["@id"]
                pos1 = params.find("?")
                params = params[pos1:]
                next_page = True

    else:
        # TODO: Inform the user that the sources could not be contacted.
        pass


    if logger:
        mgs = {
            "requests" : count_requests,
            "tuples" : card_sum,
            "querypath" : path + params,
            "timestamp" : str(datetime.datetime.now())
        }
        logger.info(str(mgs))

    queue.put("EOF")
