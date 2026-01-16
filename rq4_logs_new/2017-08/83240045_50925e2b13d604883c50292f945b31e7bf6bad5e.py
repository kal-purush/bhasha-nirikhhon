from ...common.decorators import *
from ...common.errors import *
from .in_action_query import InActionQuery, InActionQueryValue, InActionQueryResult
from decimal import *
import json
import dateutil.parse
from typing import Sequence

class MICombinator(self, topology_positions: Sequence[int])
    def __init__(self, topology_positions):
        self.topology_positions = topology_positions


class MIAbstractOperator()
    def __init__(self, *args)
        self.operands = list(args)

    def to_string(self):
        return ''

    def _to_string(self, func_name):
        val = func_name + '('
        oper_arr = []
        for oper in self.operands:
            if issubclass(type(oper), MIAbstractOperator):
                oper_arr.append(oper.to_string)
            elif type(oper) == int:
                oper_arr.append(str(oper))
            else:
                raise SerializationError(MIAbstractOperator, 'str')
        val += ','.join(oper_arr)
        val += ')'
        return val

class MIIntersection(MIAbstractOperator):
    def __init__(self, lhs, rhs):
        super().__init__(lhs, rhs)

    def to_string(self):
        return self._to_string('intersect')

def MIOutersection(MIAbstractOperator):
    def __init__(self, lhs, rhs):
        super().__init__(lhs, rhs)

    def to_string(self):
        return self._to_string('outersect')




class MultiInstanceQuery:
    def __init__(self,
                 query_set: Sequence[InActionQuery],
                 combinator_set: Sequence[MICombinator],
                 operation: str,
                 resultfilter: Sequence[int]):
        self.query_set = query_set,
        self.combinator_set = combinator_set
        self.operation = operation
        self.resultfilter = resultfilter

    @staticmethod
    def from_dict(dct):
        retval = None
        try:
            self.query_set = list(map(lambda x: InActionQuery.from_dict(x), dct['queryset']))
            self.combinator_set = list(map(lambda x: MICombinator(x), dct['combinators']))
            self.operation = dct['operation']
            self.resultfilter = dct['resultfilter']
        except Exception as e:
            raise DeserializationError(MultiInstanceQuery, 'dict', dct) from e
        return retval

    @staticmethod
    def from_json(json_string):
        query = None
        try:
            data = json.loads(json_string)
            query = MultiInstanceQuery.from_dict(data)
        except Exception as e:
            raise DeserializationError(MultiInstanceQuery, 'json', json_string) from e
        return query

    def to_dict(self):
        dct = None
        try:
            dct = {}
            dct['queryset'] = list(map(lambda x: x.to_dict(), self.query_set))
            dct['combinators'] = list(map(lambda x: x.topology_positions, self.combinator_set))
            dct['operation'] = x.operation
            dct['resultfilter'] = self.resultfilter
        except Exception as e:
            raise SerializationError(MultiInstanceQuery, 'dict') from e
        return dict

    def to_json(self):
        json_string = ""
        try:
            dct = self.to_dict()
            json_string = json.dumps(dct)
        except Exception as e:
            raise SerializationError(MultiInstanceQuery, 'json') from e
        return json_string
