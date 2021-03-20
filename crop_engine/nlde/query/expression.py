unaryFunctor = {
    '!',
    'BOUND',
    'bound',
    'ISIRI',
    'isiri',
    'ISURI',
    'isuri',
    'ISBLANK',
    'isblank',
    'ISLITERAL',
    'isliteral',
    'STR',
    'str',
    'UCASE',
    'ucase',
    'LANG',
    'lang',
    'DATATYPE',
    'datatype',
    'xsd:double',
    'xsd:integer',
    'xsd:decimal',
    'xsd:float',
    'xsd:string',
    'xsd:boolean',
    'xsd:dateTime',
    'xsd:nonPositiveInteger',
    'xsd:negativeInteger',
    'xsd:long',
    'xsd:int',
    'xsd:short',
    'xsd:byte',
    'xsd:nonNegativeInteger',
    'xsd:unsignedInt',
    'xsd:unsignedShort',
    'xsd:unsignedByte',
    'xsd:positiveInteger',
    '<http://www.w3.org/2001/XMLSchema#integer>',
    '<http://www.w3.org/2001/XMLSchema#decimal>',
    '<http://www.w3.org/2001/XMLSchema#double>',
    '<http://www.w3.org/2001/XMLSchema#float>',
    '<http://www.w3.org/2001/XMLSchema#string>',
    '<http://www.w3.org/2001/XMLSchema#boolean>',
    '<http://www.w3.org/2001/XMLSchema#dateTime>',
    '<http://www.w3.org/2001/XMLSchema#nonPositiveInteger>',
    '<http://www.w3.org/2001/XMLSchema#negativeInteger>',
    '<http://www.w3.org/2001/XMLSchema#long>',
    '<http://www.w3.org/2001/XMLSchema#int>',
    '<http://www.w3.org/2001/XMLSchema#short>',
    '<http://www.w3.org/2001/XMLSchema#byte>',
    '<http://www.w3.org/2001/XMLSchema#nonNegativeInteger>',
    '<http://www.w3.org/2001/XMLSchema#unsignedInt>',
    '<http://www.w3.org/2001/XMLSchema#unsignedShort>',
    '<http://www.w3.org/2001/XMLSchema#unsignedByte>',
    '<http://www.w3.org/2001/XMLSchema#positiveInteger>'
}
binaryFunctor = {
    'REGEX',
    'SAMETERM',
    'LANGMATCHES',
    'CONTAINS',
    'langMatches',
    'regex',
    'sameTerm'
}


class Expression(object):

    def __init__(self, op, left, right):
        self.op = op
        self.left = left
        self.right = right

    def __repr__(self):
        if (self.op in unaryFunctor):
            return (self.op + "(" + str(self.left) + ")")
        elif (self.op in binaryFunctor):
            if (self.op == 'REGEX' and self.right.desc != False):
                return (self.op + "(" + str(self.left) + "," + self.right.name + "," + self.right.desc + ")")
            else:
                return (self.op + "(" + str(self.left) + "," + str(self.right) + ")")
        elif (self.right is None):
            return (self.op + str(self.left))
        else:
            return ("(" + str(self.left) + " " + self.op + " " + str(self.right) + ")")

    def getVars(self):
        if ((self.op in unaryFunctor) or (self.op in binaryFunctor) or (self.right is None)):
            return self.left.getVars()
        else:
            return self.left.getVars() + self.right.getVars()

    def instantiate(self, d):
        return Expression(self.op, self.left.instantiate(d),
                          self.right.instantiate(d))

    def instantiateFilter(self, d, filter_str):
        return Expression(self.op, self.left.instantiateFilter(d, filter_str),
                          self.right.instantiateFilter(d, filter_str))

    def allTriplesGeneral(self):
        return False

    def allTriplesLowSelectivity(self):
        return True

    def setGeneral(self, ps, genPred):
        return

    def places(self):
        if ((self.op in unaryFunctor) or (self.op == 'REGEX' and self.right.desc == False)):
            return self.left.places()
        else:
            return self.left.places() + self.right.places()

    def constantNumber(self):
        if ((self.op in unaryFunctor) or (self.op == 'REGEX' and self.expr.desc == False)):
            return self.left.constantNumber()
        else:
            return self.left.constantNumber() + self.right.constantNumber()

    def constantPercentage(self):
        return self.constantNumber() / self.places()