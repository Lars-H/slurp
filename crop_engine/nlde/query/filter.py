class Filter(object):
    def __init__(self, expr):
        self.expr = expr

    def __repr__(self):
        if (self.expr.op == 'REGEX' or self.expr.op == 'sameTERM' or self.expr.op == 'langMATCHES'):
            if (self.expr.op == 'REGEX' and self.expr.right.desc != False):
                return "\n" + "FILTER " + self.expr.op + "(" + str(
                    self.expr.left) + "," + self.expr.right.name + "," + self.expr.right.desc + ")"
            else:
                return "\n" + "FILTER " + self.expr.op + "(" + str(self.expr.left) + "," + str(self.expr.right) + ")"
        else:
            return "\n" + "FILTER (" + str(self.expr) + ")"

    def show(self, x):
        if (self.expr.op == 'REGEX'):
            if (self.expr.right.desc != False):
                return "\n" + "FILTER " + self.expr.op + "(" + str(
                    self.expr.left) + "," + self.expr.right.name + "," + self.expr.right.desc + ")"
            else:
                return "\n" + x + "FILTER regex(" + str(self.expr.left) + "," + str(self.expr.right) + ")"
        else:
            return "\n" + x + "FILTER (" + str(self.expr) + ")"

    def getVars(self):
        return self.expr.getVars()

    def getVarsName(self):
        vars = []
        for v in self.expr.getVars():
            vars.append(v[1:len(v)])
        return vars

    def instantiateFilter(self, d, filter_str):
        return Filter(self.expr.instantiateFilter(d, filter_str))


