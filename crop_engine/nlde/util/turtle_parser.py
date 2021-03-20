'''
Created on Aug 2020

@author: Lars Heling
'''

import rdflib

hydra_prefix = "http://www.w3.org/ns/hydra/core#"

def parse_response(template, answers, var, queue, server, count, context=None, binding={}):
    card = 0
    #print template, var
    answers = rdflib.Graph().parse(data=answers.decode("utf8"), format="turtle")

    if template == 4:
        card = parseVarS(answers, var, queue, server, count, context, binding)
    elif template == 2:
        card = parseVarP(answers, var, queue, server, count, context, binding)
    elif template == 1:
        card = parseVarO(answers, var, queue, server, count, context, binding)
    elif template == 6:
        card = parseVarSP(answers, var, queue, server, count, context, binding)
    elif template == 5:
        card = parseVarSO(answers, var, queue, server, count, context, binding)
    elif template == 3:
        card = parseVarPO(answers, var, queue, server, count, context, binding)
    elif template == 0:
        card = parseNoVar(answers, var, queue, server, count, context, binding)
    elif template == 7:
        card = parseVarSPO(answers, var, queue, server, count, context, binding)

    return card



def parseNoVar(answers, var, queue, server, count, context, binding={}):
    card = 0
    if var and isinstance(var, dict):
        if len(var.get('s', [])) == 1:
            return parseVarS(answers, var['s'], queue, server, count, context)
        elif len(var.get('p', [])) == 1:
            return parseVarP(answers, var['p'], queue, server, count, context)
        elif len(var.get('o', [])) == 1:
            return parseVarO(answers, var['o'], queue, server, count, context)
    return card

def parseVarSPO(answers, var, queue, server, count, context, binding={}):
    card = 0
    for s, p, o in answers:
        # Skip Metadata
        if str(p).startswith(hydra_prefix) or str(s).startswith(server):
            continue
        #s = str(s)
        #p = str(p)
        #o = str(o)

        s = s.encode("utf-8")
        p = p.encode("utf-8")
        o = o.encode("utf-8")

        card = card + 1
        res = {var[0]: s, var[1]: p,  var[2]: o}
        res.update(binding)
        queue.put(res)
    return card

def parseVarS(answers, var, queue, server, count, context, binding={}):
    card = 0
    for s, p, o in answers:
        # Skip Metadata
        if str(p).startswith(hydra_prefix) or str(s).startswith(server):
            continue
        #s = str(s)
        s = s.encode("utf-8")
        card = card + 1
        res = {var[0]: s}
        res.update(binding)
        queue.put(res)
    return card


def parseVarP(answers, var, queue, server, count, context, binding={}):
    card = 0
    for s, p, o in answers:
        # Skip Metadata
        if str(p).startswith(hydra_prefix) or str(s).startswith(server):
            continue
        #p = str(p)
        p = p.encode("utf-8")
        card = card + 1
        res = {var[0]: p}
        res.update(binding)
        queue.put(res)
    return card


def parseVarO(answers, var, queue, server, count, context, binding={}):
    card = 0
    for s, p, o in answers:
        # Skip Metadata
        if str(p).startswith(hydra_prefix) or str(s).startswith(server):
            continue

        #o = str(o)
        o = o.encode("utf-8")
        card = card + 1
        res = {var[0]: o}
        res.update(binding)
        queue.put(res)
    return card


def parseVarSP(answers, var, queue, server, count, context, binding={}):
    card = 0
    for s, p, o in answers:
        # Skip Metadata
        if str(p).startswith(hydra_prefix) or str(s).startswith(server):
            continue
        #s = str(s)
        #p = str(p)
        s = s.encode("utf-8")
        p = p.encode("utf-8")
        card = card + 1
        res = {var[0]: s, var[1]: p}
        res.update(binding)
        queue.put(res)
    return card


def parseVarSO(answers, var, queue, server, count, context, binding={}):
    card = 0
    for s, p, o in answers:
        # Skip Metadata
        if str(p).startswith(hydra_prefix) or str(s).startswith(server):
            continue
        #s = str(s)
        #o = str(o)
        s = s.encode("utf-8")
        o = o.encode("utf-8")
        card = card + 1
        res = {var[0]: s, var[1]: o}
        res.update(binding)
        queue.put(res)
    return card


def parseVarPO(answers, var, queue, server, count, context, binding={}):
    card = 0
    for s, p, o in answers:
        # Skip Metadata
        if str(p).startswith(hydra_prefix) or str(s).startswith(server):
            continue
        #p = str(p)
        #o = str(o)
        p = p.encode("utf-8")
        o = o.encode("utf-8")

        card = card + 1
        res = {var[0]: p, var[1]: o}
        res.update(binding)
        queue.put(res)
    return card
