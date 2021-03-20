from nlde.query.triple_pattern import TriplePattern
from nlde.query.filter import Filter
from nlde.query.optional import Optional


class JoinBlock(object):

    def __init__(self, triples, filters=[], filters_str=''):
        self.triples = triples
        self.filters = filters
        self.filters_str = filters_str



    def __repr__(self):
        r = ""
        if isinstance(self.triples, list):
            for t in sorted(self.triples):
                if isinstance(t, list):
                    r = r + " ".join(map(repr, t))
                elif t:
                    if r:
                        r = r + " " + repr(t)
                    else:
                        r = repr(t)
        else:
            r = repr(self.triples)
        return r

    def __str__(self):
        return self.show(None)

    def show(self, x):
        if isinstance(self.triples, list):
            joinBody=""
            for j in sorted(self.triples):
                if isinstance(j,list):
                  if joinBody:
                     joinBody= joinBody + ". ".join(map(str,j))
                  else:
                     joinBody= joinBody + ". ".join(map(str,j))
                else:
                  if joinBody:
                     joinBody=joinBody + " " + str(j)
                  else:
                     joinBody=joinBody + " " + str(j)
            return joinBody
        else:
            return self.triples.show(x)

    @property
    def triple_pattern_count(self):
        return len(self.triples)

    @property
    def bgp(self):
        for triple in self.triples:
            if not (isinstance(triple, JoinBlock) or isinstance(triple, TriplePattern) or isinstance(triple, Filter)):
                return False
        return True

    @property
    def triple_patterns(self):
        tps = []
        for triple in self.triples:
            if isinstance(triple, TriplePattern):
                tps.append(triple)
        return tps

    @property
    def optionals(self):
        opts = []
        for triple in self.triples:
            if isinstance(triple, Optional):
                opts.append(triple)
        return opts

    @property
    def cardinality(self):
        cards = []
        for triple in self.triple_patterns:
            cards.append(triple.cardinality)
        return min(cards)