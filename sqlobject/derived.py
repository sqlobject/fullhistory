from col import ForeignKey, Col, SOCol
from sqlbuilder import AND, SQLConstant
import classregistry

class SODerived(SOCol):
    def __init__(self, *args, **kw):
        kw['derived'] = True
        kw['immutable'] = True
        super(SODerived, self).__init__(*args, **kw)

class SOFilterFrom(SODerived):
    def __init__(self, joinName=None, query=None, **kw):
        super(SOFilterFrom, self).__init__(**kw)
        self.join = joinName
        self.query = query
    
    def selectResults(self, obj):
        name = self.join
        sresults = None
        for j in obj.sqlmeta.joins:
            if j.joinMethodName == name:
                sresults = j.performJoin(obj)
                break
        if sresults is None:
            join = obj.sqlmeta.columnsExtended.get(name, None)
            if join:
                sresults = join.selectResults(obj)
        assert sresults is not None, "Could not find join '%s' in %s" % (name, obj)
        return sresults.filter(self.query)
    
    def __get__(self, obj, type=None):
        return self.selectResults(obj)

class FilterFrom(Col):
    baseClass = SOFilterFrom
    
    def __init__(self, joinName=None, query=None, **kw):
        super(FilterFrom, self).__init__(joinName=joinName, query=query, **kw)

class SOOneFrom(SOFilterFrom):
    def __get__(self, obj, type=None):
        if self.notNone:
            return self.selectResults(obj).getOne()
        else:
            return self.selectResults(obj).getOne(None)

class OneFrom(FilterFrom):
    baseClass = SOOneFrom

class SOAggregateFrom(SOFilterFrom):
    aggregateName = None
    def __init__(self, joinName=None, column=None, query=None, **kw):
        kw['joinName'] = joinName
        kw['query'] = query
        super(SOAggregateFrom, self).__init__(**kw)
        self.aggregateColumn = column

    def __get__(self, obj, type=None):
        if self.aggregateName is None:
            raise ValueError, ("Missing aggregateName, too abstract")
        arg = []
        if self.aggregateColumn:
            arg.append(self.aggregateColumn)
        return getattr(self.selectResults(obj), self.aggregateName)(*arg)

class AggregateFrom(FilterFrom):
    baseClass = SOAggregateFrom
    
    def __init__(self, joinName=None, column=None, query=None, **kw):
        super(AggregateFrom, self).__init__(joinName=joinName, column=column, query=query, **kw)

class SOCountFrom(SOAggregateFrom):
    aggregateName = "count"

class CountFrom(AggregateFrom):
    baseClass = SOCountFrom

class SOMinFrom(SOAggregateFrom):
    aggregateName = "min"

class MinFrom(AggregateFrom):
    baseClass = SOMinFrom

class SOMaxFrom(SOAggregateFrom):
    aggregateName = "max"

class MaxFrom(AggregateFrom):
    baseClass = SOMaxFrom

class SOAvgFrom(SOAggregateFrom):
    aggregateName = "avg"

class AvgFrom(AggregateFrom):
    baseClass = SOAvgFrom

class SOSumFrom(SOAggregateFrom):
    aggregateName = "sum"

class SumFrom(AggregateFrom):
    baseClass = SOSumFrom

class SOValueFrom(SODerived):
    def __init__(self, sourceColumn=None, valueColumn=None, **kw):
        super(SOValueFrom, self).__init__(**kw)
        self.sourceColumn = sourceColumn
        self.valueColumn = valueColumn

    def __get__(self, obj, type=None):
        return getattr(getattr(obj, self.sourceColumn), self.valueColumn)

class ValueFrom(Col):
    baseClass = SOValueFrom
    
    def __init__(self, sourceColumn=None, valueColumn=None, **kw):
        super(ValueFrom, self).__init__(sourceColumn=sourceColumn, valueColumn=valueColumn, **kw)
    
SQLForeignKey = ForeignKey

#~ all = []
#~ for key, value in globals().items():
    #~ if isinstance(value, type) and (issubclass(value, Col) or issubclass(value, SOCol)):
        #~ all.append(key)
#~ __all__.extend(all)
#~ del all