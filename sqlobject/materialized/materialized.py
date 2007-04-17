from sqlobject import SQLObject, sqlmeta as _sqlmeta
from sqlobject import events
from dependency import *

class MaterializedSQLObject(SQLObject):
    
    class sqlmeta(_sqlmeta):
        cachedIn = None
    
        def setClass(cls, soClass):
            super(cls, sqlmeta).setClass(soClass)
            
            if cls.cachedIn is None:
                cls.cachedIn = cls.table + '_cache'
            cls.cacheClass = type(cls.soClass.__name__+'Cache', (SQLObject,))
            
        setClass = classmethod(setClass)

def _addColumn(cls, connection, column_name, column_definition, changeSchema, post_funcs):
    def f(cls, col):
        if col.foreignKey:
            dep.add((cls.__name__, col.name), (col.foreignKey, 'id', lambda cls=cls,col=col: getattr(cls.q, col.origName)))
    post_funcs.append(f)

events.listen(_addColumn, MaterializedSQLObject, events.AddColumnSignal)

def _addJoin(cls, join_name, join_definition, post_funcs):
    def f(cls, join):
        dep.add((cls.__name__, join.joinMethodName), (join.otherClassName, 'id', lambda cls=cls,join=join: getattr(cls.q, join.joinMethodName)))
    post_funcs.append(f)

events.listen(_addJoin, MaterializedSQLObject, events.AddJoinSignal)


class MaterializedAttr(object):
    def __init__(self, func, colType, name=None):
        self.func = func
        self.colType = colType
        if name is None:
            self.name = self.func.__name__
        else:
            self.name = name
        
        self.__name__ = self.name
        self.__dict__.update(func.__dict__)
    
    def __call__(self, obj):
        val = None
        if getattr(obj, '_SO_cacheInstance', None):
            val = getattr(obj._SO_cacheInstance, self.name)
        if val is None:
            val = self.func(obj)
        return val
    
def cachedAs(colType, name=None):
    def decorate(func):
        return MaterializedAttr(func, colType, name=name)
    return decorate



def _processCacheClass(new_class_name, bases, new_attrs, post_funcs, early_funcs):
    def f(cls):
        for name, value in new_attrs.items():
            if isinstance(value, MaterializedAttr):
                cls.sqlmeta.
    post_funcs.append(f)

listen(_processCacheClass, MaterializedSQLObject, ClassCreateSignal)        