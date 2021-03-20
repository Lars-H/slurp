from urlparse import urlparse

class Query(object):
    def __init__(self, prefixes, projection, where, distinct, order_by=[], limit="", offset=""):
        self.prefixes = prefixes
        self.projection = projection
        self.where = where
        self.distinct = distinct
        self.order_by = order_by
        self.limit = limit
        self.offset = offset
        self.form = "SELECT"

    def __repr__(self):

        # Defaults.
        distinct = ""
        variables = "*"
        order_by = ""
        limit = ""
        offset = ""

        # Prefixes.
        prefixes = self.print_prefixes()

        # Distinct.
        if self.distinct:
            distinct = "DISTINCT "

        # Projection.
        if len(self.projection) > 0:
            variables = " ".join(list(map(str, self.projection)))

        # Where clause.
        where = "{\n" + str(self.where) + "}"

        if len(self.order_by) > 0:
            order_by = "ORDER BY" + " ".join(self.order_by)

        if self.limit:
            limit = "LIMIT " + self.limit

        if self.offset:
            offset = "OFFSET " + self.offset

        return "{0}{1} {2}{3}\nWHERE {4}{5}\n{6}\n{7}\n".format(
            prefixes, self.form, distinct, variables, where, order_by, limit, offset)


    def __str__(self):
        return repr(self)

    def print_prefixes(self):

        prefixes_str = ""

        for p in self.prefixes:
            prefixes_str = prefixes_str + "prefix " + p + "\n"

        return prefixes_str


class GroupGraphPattern(object):

    def __init__(self, left, right=None, join=False, union=False, optional=False):
        self.left = left
        self.right = right
        self.join = join
        self.union = union
        self.optional = optional

    def __repr__(self):

        operator = "."

        if self.union:
            operator = "UNION"
            left = "{" + str(self.left) + "}\n"
        elif self.optional:
            operator = "OPTIONAL"
            left = str(self.left) + "\n"

        if self.right:
            return left + operator + "\n{" + str(self.right) + "}\n"
        else:
            return str(self.left)


class TriplesBlock(object):
    def __init__(self, triple_patterns):
        self.triple_patterns = triple_patterns

    def __repr__(self):
        return "\n".join(list(map(str, self.triple_patterns)))


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
        return hash(self.__key())

    def __repr__(self):
        return str(self.subject) + " " + str(self.predicate) + " " + str(self.object) + " ."

    def __str__(self):
        return repr(self)

    def to_dict(self):
        return {
            "type" : "TriplePattern",
            "s" : str(self.subject),
            "p" : str(self.predicate),
            "o" : str(self.object),
            "cardinality": self.cardinality,
            "selectivity": self.selectivity
        }

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
        return len((self.get_variables().intersection(T.get_variables()))) > 0

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


class Argument(object):
    def __init__(self, value, isconstant=False):
        self.value = value
        self.isconstant = self.isuri() or self.isliteral()

    def __repr__(self):
        return str(self.value)

    def isuri(self):
        if self.value[0] == "<":
            return True
        else:
            return False

    def isvariable(self):
        if (self.value[0] == "?") or (self.value[0] == "$"):
            return True
        else:
            return False

    def isbnode(self):
        if self.value[0] == "_":
            return True
        else:
            return False

    def isliteral(self):
        if self.value[0] == '"' or self.value[0].isdigit():
            return True
        else:
            return False

    def isfloat(self):
        if self.value[0].isdigit():
            if "." in self.value:
                return True
        return False

    def isint(self):
        if self.value[0].isdigit():
            if not "." in self.value:
                return True
        return False

    def get_variable(self):
        if self.isvariable():
            return self.value[1:]

class Filter(object):
    def __init__(self):
        pass


class Expression(object):
    def __init__(self):
        pass