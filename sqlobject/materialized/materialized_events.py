from materialized import MaterializedSQLObject, MaterializedAttr
from dependency import dep
from sqlobject import events, derived
from sqlobject.main import makeProperties

####
# auto from columns

def _addColumn(cls, connection, column_name, column_definition, changeSchema, post_funcs):
    ''' for automatically adding dependencies to foreign keys'''
    def f(cls, col):
        if col.foreignKey:
            dep.add((cls.__name__, col.name), (col.foreignKey, 'id', lambda cls=cls,col=col: getattr(cls.q, col.origName)))
        if col.derived:
            pass
    post_funcs.append(f)

events.listen(_addColumn, MaterializedSQLObject, events.AddColumnSignal)

def _addJoin(cls, join_name, join_definition, post_funcs):
    ''' for automatically adding dependencies to joins'''
    def f(cls, join):
        dep.add((cls.__name__, join.joinMethodName), (join.otherClassName, 'id', lambda cls=cls,join=join: getattr(cls.q, join.joinMethodName)))
    post_funcs.append(f)

events.listen(_addJoin, MaterializedSQLObject, events.AddJoinSignal)

####
# cachedAs

def _addColumnsToCacheClass(new_class_name, bases, new_attrs, post_funcs, early_funcs):
    def f(cls):
        for name, value in new_attrs.items():
            if isinstance(value, MaterializedAttr):
                cls.sqlmeta.addCacheColumn(value)
    post_funcs.extend((f,makeProperties))

events.listen(_addColumnsToCacheClass, MaterializedSQLObject, events.ClassCreateSignal)

