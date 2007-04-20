from sqlobject import SQLObject, SQLObjectNotFound, sqlmeta as _sqlmeta
from sqlobject import events, col
from sqlobject.main import makeProperties
from dependency import dep
from sqlobject.events import *

class materialized_sqlmeta(_sqlmeta):
    cachedIn = None

    def setClass(cls, soClass):
        super(materialized_sqlmeta, cls).setClass(soClass)
        
        cls.cachedAttributes = {}
        
        cachedIn = cls.cachedIn
        if cachedIn is None:
            cachedIn = cls.table + '_cache'
        class cache_sqlmeta:
            table = cachedIn
            idType = cls.idType
            
        cls.cacheClass = type(cls.soClass.__name__+'Cache', (SQLObject,), {'sqlmeta': cache_sqlmeta})
        
    setClass = classmethod(setClass)

class MaterializedSQLObject(SQLObject):
    
    sqlmeta = materialized_sqlmeta
    
    def _init(self, id, connection=None, **kw):
        super(MaterializedSQLObject, self)._init(id, connection=connection, **kw)
        try:
            self._SO_cacheObject = self.sqlmeta.cacheClass.get(self.id, connection=self._connection)
        except SQLObjectNotFound:
            print self.sqlmeta.cacheClass._connection, connection
            self._SO_cacheObject = self.sqlmeta.cacheClass(id=self.id, connection=self._connection)
    

    def _SO_updateCacheObject(self, attrs):
        for attr in attrs:
            cacheAttr = self.sqlmeta.cachedAttributes.get(attr, None)
            if cacheAttr:
                new_value = cacheAttr.func(self)
                self._SO_cacheObject.set(**{attr+'_dirty': False,
                                            attr: new_value})

def _addColumn(cls, connection, column_name, column_definition, changeSchema, post_funcs):
    ''' for automatically adding dependencies to foreign keys'''
    def f(cls, col):
        if col.foreignKey:
            dep.add((cls.__name__, col.name), (col.foreignKey, 'id', lambda cls=cls,col=col: getattr(cls.q, col.origName)))
    post_funcs.append(f)

events.listen(_addColumn, MaterializedSQLObject, events.AddColumnSignal)

def _addJoin(cls, join_name, join_definition, post_funcs):
    ''' for automatically adding dependencies to joins'''
    def f(cls, join):
        dep.add((cls.__name__, join.joinMethodName), (join.otherClassName, 'id', lambda cls=cls,join=join: getattr(cls.q, join.joinMethodName)))
    post_funcs.append(f)

events.listen(_addJoin, MaterializedSQLObject, events.AddJoinSignal)


class MaterializedAttr(object):
    def __init__(self, func, colType, name=None):
        self.func = func
        self.colType = colType
        
        if name is None:
            name = self.func.__name__
            if name[:5] == '_get_':
                name = name[5:]
        self.name = name
        
        self.__name__ = self.name
        self.__dict__.update(func.__dict__)
    
    def __call__(self, obj):
        val = None
        if getattr(obj, '_SO_cacheObject', None):
            if not getattr(obj._SO_cacheObject, self.name+"_dirty"):
                val = getattr(obj._SO_cacheObject, self.name)
        if val is None:
            val = self.func(obj)
        return val
    
def cachedAs(colType, name=None):
    def decorate(func):
        return MaterializedAttr(func, colType, name=name)
    return decorate


def _addColumnsToCacheClass(new_class_name, bases, new_attrs, post_funcs, early_funcs):
    def f(cls):
        for name, value in new_attrs.items():
            if isinstance(value, MaterializedAttr):
                cls.sqlmeta.cachedAttributes[value.name] = value
                cls.sqlmeta.cacheClass.sqlmeta.addColumn(value.colType(name=value.name, default=None))
                cls.sqlmeta.cacheClass.sqlmeta.addColumn(col.BoolCol(name=value.name+'_dirty', default=True))
    post_funcs.extend((f,makeProperties))

listen(_addColumnsToCacheClass, MaterializedSQLObject, ClassCreateSignal)
