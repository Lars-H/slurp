"""
Created on Sep 03, 2017

@author: Maribel Acosta
"""
from abc import ABCMeta, abstractmethod


class Policy(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def initialize_priorities(self, init_priorities):
        pass

    @abstractmethod
    def select_operator(self, operators, operators_desc, tup, operators_vars, operators_not_sym):
        pass

    @abstractmethod
    def update_priorities(self, t, table):
        pass
