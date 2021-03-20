from urlparse import urlparse
from . import Argument

class TriplePattern(object):

    def __init__(self, s, p, o, **kwargs):

        if isinstance(s, Argument):
            self.subject = s
        else:
            self.subject = Argument(s)

        if isinstance(p, Argument):
            self.predicate = p
        else:
            self.predicate = Argument(p)

        if isinstance(o, Argument):
            self.object = o
        else:
            self.object = Argument(o)


        self.count = kwargs.get("count", None)
        self.distinct_subjects = kwargs.get("subjects", None)
        self.distinct_predicates = kwargs.get("predicates", None)
        self.distinct_objects = kwargs.get("objects", None)
        self.sources = kwargs.get("sources", {})

        self.subject_auths = {}
        self.object_auths = {}
        self.id = -1

    def __key(self):
        return (self.subject, self.predicate, self.object)

    def __hash__(self):
        return hash(str(self))

    def __repr__(self):
        return str(self.subject) + " " + str(self.predicate) + " " + str(self.object) + " ."

    def __str__(self):
        return repr(self)

    # Less than: for Sorting
    def __lt__(self, other):
        return hash(self) < hash(other)

    # Equals: for Sorting
    def __eq__(self, other):
        return hash(self) == hash(other)

    # Length
    def __len__(self):
        return 1

    @property
    def cardinality(self):
        return self.count

    @cardinality.setter
    def cardinality(self, cardinality):
        self.count = cardinality

    @property
    def selectivity(self):
        try:
            sel = min(
                float(1.0 / self.distinct_subjects),
                float(1.0 / self.distinct_predicates),
                float(1.0 / self.distinct_objects))
            return sel
        except:
            return 0.0

    def get_variables(self):
        variables = []

        if not self.subject.isconstant:
            variables.append(self.subject.get_variable())

        if not self.predicate.isconstant:
            variables.append(self.predicate.get_variable())

        if not self.object.isconstant:
            variables.append(self.object.get_variable())

        return set(variables)

    def __getitem__(self, i):
        if i == 0:
            return self.subject
        elif i == 1:
            return self.predicate
        elif i == 2:
            return self.object
        elif i == 3:
            return self.variables
        else:
            raise IndexError()

    @property
    def variables(self):
        return self.get_variables()

    def compatible(self, T):
        return len((self.variables.intersection(T.variables))) > 0

    @property
    def variables_dict(self):
        v_dict = {
            "s" : [],
            "p" : [],
            "o" : []
        }

        if not self.subject.isconstant:
            v_dict['s'] = list([self.subject.get_variable()])

        if not self.predicate.isconstant:
            v_dict['p'] = list([self.predicate.get_variable()])

        if not self.object.isconstant:
            v_dict['o'] = list([self.object.get_variable()])

        return v_dict

    def get_variable_position(self, var):
        positions = 0

        if not self.subject.isconstant and self.subject.get_variable() == var:
            positions = positions | 4

        if not self.predicate.isconstant and self.predicate.get_variable() == var:
            positions = positions | 2

        if not self.object.isconstant and self.object.get_variable() == var:
            positions = positions | 1

        return positions

    @property
    def variable_position(self):
        positions = 0

        if not self.subject.isconstant and self.subject.isvariable():
            positions = positions | 4

        if not self.predicate.isconstant and self.predicate.isvariable():
            positions = positions | 2

        if not self.object.isconstant and self.object.isvariable():
            positions = positions | 1
        return positions


    @property
    def source_set(self):
        return set(self.sources.keys())