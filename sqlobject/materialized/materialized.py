from sqlobject import SQLObject, SQLObjectNotFound, sqlmeta as _sqlmeta
from sqlobject import col

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
    
    def addCacheColumn(self, value):
        self.cachedAttributes[value.name] = value
        self.cacheClass.sqlmeta.addColumn(value.colType(name=value.name, default=None))
        self.cacheClass.sqlmeta.addColumn(col.BoolCol(name=value.name+'_dirty', default=True))
    addCacheColumn = classmethod(addCacheColumn)
    
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
        changed = []
        for attr in attrs:
            cacheAttr = self.sqlmeta.cachedAttributes.get(attr, None)
            if cacheAttr:
                new_value = cacheAttr.func(self)
                if new_value != getattr(self._SO_cacheObject, attr):
                    changed.append(attr)
                    self._SO_cacheObject.set(**{attr+'_dirty': False,
                                                attr: new_value})
            else:
                # If it's not cached, assume descendents should be notified regardless.
                # Currently this should be mostly for joins, if we decide to cache joins
                # then they would appear changed in most cases here anyways (add, delete).
                changed.append(attr)
        return changed

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
