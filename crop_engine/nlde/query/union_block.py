from .util import nest, aux
from .join_block import JoinBlock

class UnionBlock(object):
    def __init__(self, triples, filters=[]):
        self.triples = triples
        self.filters = filters

    def __repr__(self):

        if len(self.triples) == 1:
            return repr(self.triples[0])
        else:
            return " UNION ".join(repr(block) for block in self.triples)

    def show(self, w):

        n = nest(self.triples)
        if n:
            return aux(n, w, " UNION ") + " ".join(map(str, self.filters))
        else:
            return " "

    def setGeneral(self, ps, genPred):
        if isinstance(self.triples, list):
            for t in self.triples:
                t.setGeneral(ps, genPred)
        else:
            self.triples.setGeneral(ps, genPred)

    def allTriplesGeneral(self):
        a = True
        if isinstance(self.triples, list):
            for t in self.triples:
                a = a and t.allTriplesGeneral()
        else:
            a = self.triples.allTriplesGeneral()
        return a

    def allTriplesLowSelectivity(self):
        a = True
        if isinstance(self.triples, list):
            for t in self.triples:
                a = a and t.allTriplesLowSelectivity()
        else:
            a = self.triples.allTriplesLowSelectivity()
        return a

    def instantiate(self, d):
        if isinstance(self.triples, list):
            ts = [t.instantiate(d) for t in self.triples]
            return JoinBlock(ts)
        else:
            return self.triples.instantiate(d)

    def instantiateFilter(self, d, filter_str):
        if isinstance(self.triples, list):
            ts = [t.instantiateFilter(d, filter_str) for t in self.triples]
            return JoinBlock(ts, filter_str)
        else:
            return self.triples.instantiateFilter(d, filter_str)

    def getVars(self):
        l = []
        for t in self.triples:
            l = l + t.getVars()
        return l

    def getPredVars(self):
        l = []
        for t in self.triples:
            l = l + t.getPredVars()
        return l

    def includeFilter(self, f):

        for t in self.triples:
            t.includeFilter(f)

    @property
    def triple_pattern_count(self):
        tp_sum = 0
        for sub_block in self.triples:
            tp_sum += sub_block.triple_pattern_count
        return tp_sum

    @property
    def cardinality(self):
        cards = []
        for triple in self.triples:
            cards.append(triple.cardinality)
        return sum(cards)