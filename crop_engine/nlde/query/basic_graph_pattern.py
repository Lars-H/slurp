
class BasicGraphPattern(object):


    def __init__(self, triple_patterns):

        self.triple_patterns = triple_patterns
        self.cardinality = 0

        self.__source_set = set()
        self.__variables = set()
        for triple_pattern in triple_patterns:
            self.__source_set.update(triple_pattern.source_set)
            self.__variables.update(triple_pattern.variables)

    def __repr__(self):
        return " ".join([str(tp) for tp in sorted(self.triple_patterns)])

    def __str__(self):
        return " ".join([str(tp) for tp in self.triple_patterns])

    def __len__(self):
        return len(self.triple_patterns)

    def __hash__(self):
        return hash(repr(self))

    def __eq__(self, other):
        return hash(self) == hash(other)

    def compatible(self, T):
        return len((self.variables.intersection(T.variables))) > 0

    @property
    def variables(self):
        return self.__variables

    @property
    def total_res(self):
        return self.cardinality

    @property
    def sources(self):
        soruces_dct = {}
        for tp in self.triple_patterns:
            soruces_dct.update(tp.sources)
        return soruces_dct

    @property
    def source_set(self):
        return self.__source_set

    def __getitem__(self, item):
        return self.triple_patterns[item]

    def __iter__(self):
        return iter(self.triple_patterns)

