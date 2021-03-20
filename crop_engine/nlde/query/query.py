import string

class Query(object):

    def __init__(self, prefs, args, body, distinct, order_by=[], limit=-1, offset=-1, filter_nested=''):
        self.prefs = prefs
        self.args = args
        self.body = body
        self.distinct = distinct
        self.order_by = order_by
        self.limit = limit
        self.offset = offset
        self.filter_nested = filter_nested

    def __hash__(self):
        return hash(repr(self))

    def __repr__(self):
        body_str = repr(self.body)
        args_str = " ".join(map(repr, self.args))
        if self.args == []:
            args_str = "*"
        if self.distinct:
            d = "DISTINCT "
        else:
            d = ""
        #return self.getPrefixes() + "SELECT " + d + args_str + "\nWHERE {" + body_str + "\n" + self.filter_nested +
        # "\n}"
        return "SELECT " + d + args_str + "\nWHERE {" + body_str + "\n" + self.filter_nested + \
               "\n}"

    def instantiate(self, d):
        new_args = []
        for a in self.args:
            an = string.lstrip(string.lstrip(self.subject.name, "?"), "$")
            if not (an in d):
                new_args.append(a)
        return Query(self.prefs, new_args, self.body.instantiate(d), self.distinct)

    def instantiateFilter(self, d, filter_str):
        new_args = []
        for a in self.args:
            an = string.lstrip(string.lstrip(self.subject.name, "?"), "$")
            if not (an in d):
                new_args.append(a)
        return Query(self.prefs, new_args, self.body, self.distinct, self.filter_nested + ' ' + filter_str)

    def show(self):

        body_str = self.body.show(" ")
        args_str = " ".join(map(str, self.args))
        if self.args == []:
            args_str = "*"
        if self.distinct:
            d = "DISTINCT "
        else:
            d = ""
        return self.getPrefixes() + "SELECT " + d + args_str + "\nWHERE {" + body_str + "\n" + self.filter_nested + "\n}"

    def getPrefixes(self):
        r = ""
        for short, long in self.prefs.items():
            r = r + "\nPREFIX " + short + " : " + long
        if not r == "":
            r = r + "\n"
        return r

    @property
    def triple_pattern_count(self):
        tp_sum = 0
        for sub_block in self.body.triples:
            tp_sum += sub_block.triple_pattern_count
        return tp_sum

    @property
    def projection(self):
        if len(self.args) == 0:
            return None
        else:
            return self.args