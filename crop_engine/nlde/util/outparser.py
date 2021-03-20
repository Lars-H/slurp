"""
Created on Mar 23, 2015

@author: Maribel Acosta
"""
from rdflib import URIRef, Literal, BNode


def rdf_repr(term):

    if term.__class__ is URIRef:
        return "<%s>" % str(term)
    elif term.__class__ is Literal:
        if term.language is not None:
            return '"%s"@%s' % (term.value, term.language)
        elif term.datatype is not None:
            return '"%s"^^%s' % (term.value, term.datatype)
        else:
            return '%s' % term.value
    elif term.__class__ is BNode:
        return "_:%s" % term

def str_repr(term):

    if term.__class__ is URIRef:
        return "%s" % str(term)
    elif term.__class__ is Literal:
        if term.language is not None:
            return '"%s"@%s' % (term.value, term.language)
        elif term.datatype is not None:
            return '"%s"^^%s' % (term.value, term.datatype)
        else:
            return '%s' % term.value
    elif term.__class__ is BNode:
        return "_:%s" % term


def ldf_parser(template, graph, tp, var, queue):

    #print "answers", answers
    if template == 4:
        parseVarS(graph, tp, var, queue)
    elif template == 2:
        parseVarP(graph, tp, var, queue)
    elif template == 1:
        parseVarO(graph, tp, var, queue)
    elif template == 6:
        parseVarSP(graph, tp, var, queue)
    elif template == 5:
        parseVarSO(graph, tp, var, queue)
    elif template == 3:
        parseVarPO(graph, tp, var, queue)
    elif template == 0:
        parseNoVar(graph, tp, queue)


def parseVarS(graph, tp, var, queue):

    for (s, p, o) in graph:
        if rdf_repr(p) == tp.predicate.value and rdf_repr(o) == tp.object.value:
            elem = {var[0]: str_repr(s)}
            print "parseVarS", elem
            queue.put(elem)


def parseVarP(graph, tp, var, queue):

    for (s, p, o) in graph:
        if rdf_repr(s) == tp.subject.value and rdf_repr(o) == tp.object.value:
            elem = {var[0]: str_repr(p)}
            print "parseVarP", elem
            queue.put(elem)


def parseVarO(graph, tp, var, queue):

    for (s, p, o) in graph:
        if rdf_repr(s) == tp.subject.value and rdf_repr(p) == tp.predicate.value:
            elem = {var[0]: str_repr(o)}
            print "parseVarO", elem
            queue.put(elem)


def parseVarSP(graph, tp, var, queue):

    for (s, p, o) in graph:
        if rdf_repr(o) == tp.object.value:
            elem = {var[0]: str_repr(s), var[1]: str_repr(p)}
            print "parseVarSP", elem
            queue.put(elem)


def parseVarSO(graph, tp, var, queue):

    for (s, p, o) in graph:
        if rdf_repr(p) == tp.predicate.value:
            elem = {var[0]: str_repr(s), var[1]: str_repr(o)}
            print "parseVarSO", elem
            queue.put(elem)


def parseVarPO(graph, tp, var, queue):

    for (s, p, o) in graph:
        if rdf_repr(s) == tp.subject.value:
            elem = {var[0]: str_repr(p), var[1]: str_repr(o)}
            print "parseVarPO", elem
            queue.put(elem)


def parseNoVar(graph, tp, queue):

    for (s, p, o) in graph:
        if rdf_repr(s) == tp.subject.value and rdf_repr(p) == tp.predicate.value and rdf_repr(o) == tp.object.value:
            queue.put({})
            return