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
            return '%s@%s' % (term.value, term.language)
        elif term.datatype is not None:
            return '%s^^%s' % (term.value, term.datatype)
        else:
            return '%s' % term.value
    elif term.__class__ is BNode:
        return "_:%s" % term.value