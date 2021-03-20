

class Optional(object):
    def __init__(self, triples):
        self.triples = triples

    def __repr__(self):
        return " OPTIONAL { " + str(self.triples) + " }"

    @property
    def triple_pattern_count(self):
        return self.triples.triple_pattern_count

    @property
    def bgp(self):
        return False
        #for triple in self.triples:
        #    if not (isinstance(triple, JoinBlock) or isinstance(triple, TriplePattern) or isinstance(triple, Filter)):
        #        return False
        #return True

    @property
    def cardinality(self):
        return self.triples.cardinality