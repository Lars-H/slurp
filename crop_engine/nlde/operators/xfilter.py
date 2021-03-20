'''
Created on July 15th 2020
Implements the Xfilter operator.
The intermediate results are represented in a queue.
@author: Lars Heling
'''
from multiprocessing import Queue, Value
from  Queue import Empty
from nlde.query import Filter, Expression, Argument
from nlde.util.misc import extractValue
import operator
from random import randint

unary_operators = {
    '!': operator.not_,
    '+': '',
    '-': operator.neg
}

logical_connectives = {
    '||': operator.or_,
    '&&': operator.and_
}

arithmetic_operators = {
    '*': operator.mul,
    '/': operator.div,
    '+': operator.add,
    '-': operator.sub,
}

test_operators = {
    '=': operator.eq,
    '!=': operator.ne,
    '<': operator.lt,
    '>': operator.gt,
    '<=': operator.le,
    '>=': operator.ge
}

numerical = (int, long, float)


class Xfilter(object):

    def __init__(self, id_operator, eddies, filter):
        self.id_operator = id_operator
        self.left = None
        self.right = None
        self.qresults = None
        self.eddies = eddies
        self.eddy = 1#randint(1, self.eddies)
        self.probing = Value('i', 1)
        self.wait = True
        self.independent_inputs = 1
        self.filter = filter

    def __str__(self):
        return "XFilter"


    def execute(self, inputs, out):
        # Executes the Xfilter.
        self.left = inputs[0]
        self.qresults = out
        # Apply fltr tuple by tuple.

        while True:

            try:
                # Get tuple (with solution mapping).
                self.probing.value = 1
                tuple = self.left.get(self.wait)

                # Perform projection in the solution mapping.
                if tuple.data != "EOF" and tuple != "EOF":
                    # Create solution mapping with the specified domain in self.vars.
                    (res, _) = self.evaluateComplexExpression(tuple.data, self.filter.expr.op,
                                                              (self.filter.expr.left, None),
                                                              (self.filter.expr.right, None))

                    if res:
                        # Put tuple with solution mapping to output queue.
                        tuple.done = tuple.done | pow(2, self.id_operator)
                        self.qresults[self.eddy].put(tuple)

                else:
                    self.wait = False
                    tuple.done = tuple.done | pow(2, self.id_operator)
                    self.qresults[self.eddy].put(tuple)

            except Empty:
                self.probing.value = 0



    # Base case.
    def evaluateOperator(self, operator, expr_left, expr_right):
        # print "operator in Filter", operator
        if (operator in unary_operators):
            # print "Case: unary_operators"
            return self.evaluateUnaryOperator(operator, expr_left)
        elif (operator in logical_connectives):
            # print "Case: logical connectives"
            return self.evaluateLogicalConnective(operator, expr_left, expr_right)
        elif (operator in arithmetic_operators):
            # print "Case: arithmetic operator"
            return self.evaluateArithmeticOperators(operator, expr_left, expr_right)
        elif (operator in test_operators):
            # print "Case: test"
            return self.evaluateTest(operator, expr_left, expr_right)

    # Inductive case.
    def evaluateComplexExpression(self, tuple, operator, (expr_left, type_left), (expr_right, type_right)):

        # Case 1: Inductive case binary operator OP(Expr, Expr)
        if isinstance(expr_left, Expression) and isinstance(expr_right, Expression):
            # print "Case 1"
            res_left = self.evaluateComplexExpression(tuple, expr_left.op,
                                                      (expr_left.left, type_left), (expr_left.right, type_right))
            res_right = self.evaluateComplexExpression(tuple, expr_right.op,
                                                       (expr_right.left, type_left), (expr_right.right, type_right))
            res = self.evaluateOperator(operator, res_left, res_right)

        # Case 2: Inductive case binary operator OP(Expr, Arg)
        elif isinstance(expr_left, Expression) and isinstance(expr_right, Argument):
            # print "Case 2"
            res_left = self.evaluateComplexExpression(tuple, expr_left.op,
                                                      (expr_left.left, type_left), (expr_left.right, type_right))
            res_right = extractValue(tuple[expr_right.name[1:]])
            res = self.evaluateOperator(operator, res_left, res_right)

        # Case 3: Inductive case binary operator OP(Arg, Expr)
        elif isinstance(expr_left, Argument) and isinstance(expr_right, Expression):
            # print "Case 3"
            res_left = extractValue(tuple[expr_left.name[1:]])
            res_right = self.evaluateComplexExpression(tuple, expr_right.op,
                                                       (expr_right.left, type_left), (expr_right.right, type_right))
            res = self.evaluateOperator(operator, res_left, res_right)

        # Case 4: Inductive case unary operator OP(Expr, None)
        elif isinstance(expr_left, Expression):
            # print "Case 4"
            res_left = self.evaluateComplexExpression(tuple, expr_left.op,
                                                      (expr_left.left, type_left), (expr_left.right, type_right))
            res = self.evaluateOperator(operator, res_left, None)

        # Case 5: Base case binary operator OP(Arg, Arg)
        elif isinstance(expr_left, Argument) and isinstance(expr_right, Argument):
            # print "Case 5"
            if expr_left.isconstant:
                res_left = extractValue(expr_left.value)
            else:
                res_left = extractValue(tuple[expr_left.name[1:]])

            if expr_right.isconstant:
                res_right = extractValue(expr_right.value)
            else:
                res_right = extractValue(tuple[expr_right.name[1:]])
            res = self.evaluateOperator(operator, res_left, res_right)

        # Case 6: Base case unary operator OP(Arg, None)
        elif isinstance(expr_left, Argument):
            # print "Case 6"
            res_left = extractValue(tuple[expr_left.name[1:]])
            res = self.evaluateOperator(operator, res_left, None)
        else:
            pass

        return res

    '''
    evaluateEBV: calculates whether an argument is an Effective Boolean Value (EBV)
                 according to the definition in the SPARQL documentation 
                 See: http://www.w3.org/TR/sparql11-query/#ebv

    input: val -- an argument
    return: (isEBV, EBV) -- both of Python type bool
    '''

    def evaluateEBV(self, casted_val, type_val):

        # Handles python data types.
        if (isinstance(casted_val, bool)):
            return (True, casted_val)
        if (isinstance(casted_val, numerical)):
            if (casted_val == 0 or casted_val == 'nan'):
                return (True, False)
            else:
                return (True, True)

        # Rule 1
        if ((type_val == bool) and (casted_val != 'true') and (casted_val != 'false')):
            return (True, False)
        elif ((type_val == 'numeric') and not (isinstance(casted_val, numerical))):
            return (True, False)

        # Rule 2
        if (type_val == bool):
            if (casted_val == 'true'):
                return (True, True)
            elif (casted_val == 'false'):
                return (True, False)

        # Rule 3
        if (type_val == str):
            if (len(casted_val) == 0):
                return (True, False)
            else:
                return (True, True)

        # Rule 4
        if ((type_val == 'numeric')):
            if (casted_val == 0 or casted_val == 'nan'):
                return (True, False)
            else:
                return (True, True)

        # Rule 5: The error type should be raised by the evaluators.
        return (False, None)

    def evaluateUnaryOperator(self, tuple, operator, (expr_left, type_left)):

        if (operator == '+' and isinstance(expr_left, numerical)):
            return (expr_left, type_left)

        elif (operator == '-' and isinstance(expr_left, numerical)):
            return (unary_operators[operator](expr_left), type_left)

        elif (operator == '!'):
            (isEBV, ebv) = self.evaluateEBV(expr_left, type_left)
            if (isEBV):
                return (unary_operators[operator](ebv), type_left)
            else:
                raise SPARQLTypeError
        else:
            raise SPARQLTypeError

    def evaluateLogicalConnective(self, operator, (expr_left, type_left), (expr_right, type_right)):

        (isEBV_left, ebv_left) = self.evaluateEBV(expr_left, type_left)
        (isEBV_right, ebv_right) = self.evaluateEBV(expr_right, type_right)

        # print "in evaluateLogicalConnective", expr_left, isEBV_left, ebv_left
        # print "in evaluateLogicalConnective", expr_right, isEBV_right, ebv_right

        if (isEBV_left and isEBV_right):
            return (logical_connectives[operator](ebv_left, ebv_right), bool)

        elif (isEBV_left):
            res = logical_connectives[operator](ebv_left, 'Error')
            if (res == 'Error'):
                raise SPARQLTypeError
            else:
                return (res, bool)

        elif (isBV_right):
            res = logical_connectives[operator](ebv_right, 'Error')
            if (res == 'Error'):
                raise SPARQLTypeError
            else:
                return (res, bool)

    def evaluateTest(self, operator, expr_left, expr_right):

        #if ((type(expr_left) == type(expr_right)) or (
        #        isinstance(expr_left, numerical) and isinstance(expr_right, numerical))):
            # print "Here", val_left, type_left, val_right, type_right
        try:
            if expr_left[1] == expr_right[1]:
                return (test_operators[operator](expr_left, expr_right), bool)
            else:
                return (False, bool)
        except:
            print "SPARQLTypeError"
            raise SPARQLTypeError

    def evaluateAritmethic(self, operator, (expr_left, type_left), (expr_right, type_right)):

        if (isinstance(expr_left, numerical) and isinstance(expr_right, numerical)):
            return (
            arithmetic_operators[operator](expr_left, expr_right), type_left)  # TODO: implement the cases with types
        else:
            raise SPARQLTypeError


class SPARQLTypeError(Exception):
    """Base class for exceptions in this module."""
    pass