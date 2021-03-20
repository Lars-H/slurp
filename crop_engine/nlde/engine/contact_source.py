from contact_tpfs import get_metadata_tpf, contact_single_tpf_server, contact_single_tpf_server_binding
from contact_sparql import contact_single_endpoint_server, get_metadata_endpoint, \
    contact_single_endpoint_server_bindings
from contact_brtpf import get_metadata_brtpf, contact_single_brtpf_server, contact_single_brtpf_server_binding
from nlde.query import TriplePattern
from nlde.operators.operatorstructures import EOF
from multiprocessing import Process, Queue


def get_metadata(servers, query):

    tpfs = []
    endpoints = []
    brtpfs = []
    for server in servers:
        if "sparql@" in server:
            server_url = server.replace("sparql@", "")
            endpoints.append(server_url)
        elif "brtpf@" in server:
            server_url = server.replace("brtpf@", "")
            brtpfs.append(server_url)
        elif "tpf@" in server:
            server_url = server.replace("tpf@", "")
            tpfs.append(server_url)
        else:
            tpfs.append(server)

    cardinality_sum = 0

    if len(tpfs) > 0:
        cardinality_sum += get_metadata_tpf(tpfs, query)
    if len(endpoints) > 0:
        cardinality_sum += get_metadata_endpoint(endpoints, query)
    if len(brtpfs) > 0:
        cardinality_sum += get_metadata_brtpf(brtpfs, query)

    if isinstance(query, TriplePattern):
        query.cardinality = cardinality_sum

    return cardinality_sum


def contact_source_direct(server, query, queue, vars=None, **kwargs):

    sparql_limit = kwargs.get("sparql_limit", 500)
    if "sparql@" in server:
        server_url = server.replace("sparql@", "")
        contact_single_endpoint_server(server_url, query, queue, vars, put_eof=False, limit=sparql_limit)
        queue.put("EOF")
    elif "brtpf@" in server:
        server_url = server.replace("brtpf@", "")
        contact_single_brtpf_server(server_url, query, queue, vars, put_eof=False)
        queue.put("EOF")
    elif "tpf@" in server:
        server_url = server.replace("tpf@", "")
        contact_single_tpf_server(server_url, query, queue, vars, put_eof=False)
        queue.put("EOF")
    else:
        server_url = server
        contact_single_tpf_server(server_url, query, queue, vars, put_eof=False)
        queue.put("EOF")


def contact_source(servers, query, queue, vars=None, **kwargs):

    #if len(query.sources.keys()) == 1:
    #    return contact_source_direct(query.sources.keys()[0], query, queue, vars, **kwargs)
    sparql_limit = kwargs.get("sparql_limit", 500)
    p_list = []
    aux_queue = Queue()
    for server in servers:
        if server in query.sources.keys():
            if "sparql@" in server:
                server_url = server.replace("sparql@", "")
                p = Process(target=contact_single_endpoint_server, args=(server_url, query, aux_queue, vars), kwargs={"limit" :
                                                                                                      sparql_limit,
                                                                                                    "put_eof" :True})
                p.start()
                p_list.append(p.pid)
            elif "brtpf@" in server:
                server_url = server.replace("brtpf@", "")
                p = Process(target=contact_single_brtpf_server, args=(server_url, query, aux_queue, vars), kwargs={"put_eof": True})
                p.start()
                p_list.append(p.pid)

            elif "tpf@" in server:
                server_url = server.replace("tpf@", "")
                p = Process(target=contact_single_tpf_server, args=(server_url, query, aux_queue, vars), kwargs={"put_eof": True})
                p.start()
                p_list.append(p.pid)
            else:
                server_url = server
                p = Process(target=contact_single_tpf_server, args=(server_url, query, aux_queue, vars), kwargs={"put_eof": True})
                p.start()
                p_list.append(p.pid)

    if len(p_list) == 0:
        queue.put("EOF")
        return None

    requests = 0
    processes_finalized = 0
    while True:
        tuple = aux_queue.get()

        if tuple != "EOF":
            queue.put(tuple)
        else:
            requests += tuple.get("requests", 0)
            processes_finalized += 1

        if processes_finalized == len(p_list):
            break

    eof = EOF(requests=requests)
    queue.put(eof)
    #queue.put("EOF")


def contact_source_bindings(servers, query, queue, bindings, vars=None, **kwargs):

    sparql_limit = kwargs.get("sparql_limit", 500)
    requests_cnts = []
    for server in servers:
        if server in query.sources.keys():
            if "sparql@" in server:
                server_url = server.replace("sparql@", "")
                requests_sparql = contact_single_endpoint_server_bindings(server_url, query, queue, bindings, vars,
                                                            limit=sparql_limit)
                requests_cnts.append(requests_sparql)

            elif "brtpf@" in server:
                server_url = server.replace("brtpf@", "")
                requests_brtpf = contact_single_brtpf_server_binding(server_url, query, queue, bindings, vars)
                requests_cnts.append(requests_brtpf)

            elif "tpf@" in server:
                server_url = server.replace("tpf@", "")
                requests_tpf = contact_single_tpf_server_binding(server_url, query, queue, bindings, vars)
                requests_cnts.append(requests_tpf)

            else:
                requests_tpf = contact_single_tpf_server_binding(server, query, queue, bindings, vars)
                requests_cnts.append(requests_tpf)

    requests_total = sum(requests_cnts)
    eof = EOF(requests=requests_total)
    queue.put(eof)
    #queue.put("EOF")