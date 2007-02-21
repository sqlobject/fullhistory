#from sqlobject.sqlbuilder import *
#from sqlobject.declarative import classinstancemethod
#from sqlobject import classregistry
#from SQLObject import SQLObject
#from sqlobject.col import Col, KeyCol
from sqlbuilder import *
from main import SQLObject, sqlmeta
import types, threading


class ColumnAS(SQLOp):
    ''' Just like SQLOp('AS', expr, name) except without the parentheses '''
    def __init__(self, expr, name):
        if isinstance(name, (str, unicode)):
            name = SQLConstant(name)  
        SQLOp.__init__(self, 'AS', expr, name)
    def __sqlrepr__(self, db):
        return "%s %s %s" % (sqlrepr(self.expr1, db), self.op, sqlrepr(self.expr2, db))

####

class ViewSQLObjectField(SQLObjectField):
    def __init__(self, alias, *arg):
        self.alias = alias
        SQLObjectField.__init__(self, *arg)
    def __sqlrepr__(self, db):
        return self.alias + "." + self.fieldName
    def tablesUsedImmediate(self):
        return [self.tableName]

class UnicodeViewSQLObjectField(UnicodeField):
    def __init__(self, alias, *arg):
        self.alias = alias
        UnicodeField.__init__(self, *arg)
    __sqlrepr__ = ViewSQLObjectField.__sqlrepr__
    tablesUsedImmediate = ViewSQLObjectField.tablesUsedImmediate

class ViewSQLObjectTable(SQLObjectTable):
    FieldClass = ViewSQLObjectField
    UnicodeFieldClass = UnicodeViewSQLObjectField
    
    def __getattr__(self, attr):
        if attr == 'sqlmeta':
            raise AttributeError
        alias = self.soClass.sqlmeta.alias
        if attr.startswith('__'):
            raise AttributeError
        if attr == 'id':
            return self.FieldClass(alias, self.tableName, 'id', attr)
        elif attr not in self.soClass.sqlmeta.columns:
            raise AttributeError("%s instance has no attribute '%s'" % (self.soClass.__name__, attr))
        else:
            column = self.soClass.sqlmeta.columns[attr]
            if hasattr(column, "dbEncoding"):
                return self.UnicodeFieldClass(alias, self.tableName, column.name,
                    attr, column)
            else:
                return self.FieldClass(alias, self.tableName, column.name, attr)

class ViewSQLObjectMeta(sqlmeta):
    pass


class ViewSQLObject(SQLObject):
    '''A SQLObject class that derives all it's values from other SQLObject classes.
        Columns on subclasses should use SQLBuilder constructs for dbName,
        and sqlmeta should specify:
            idName as a SQLBuilder construction
            clause as SQLBuilder clause for specifying join conditions or other restrictions
            alias as an optional alternate name (as table is typically used for SQLObjects)
        See test_views.py for simple examples.
    '''
    
    class sqlmeta(ViewSQLObjectMeta):
        pass
    
    def __classinit__(cls, new_attrs):
        SQLObject.__classinit__(cls, new_attrs)
        # like is_base
        if cls.__name__ != 'ViewSQLObject':
            if not getattr(cls.sqlmeta, 'alias', None):
                cls.sqlmeta.alias = cls.sqlmeta.style.pythonClassToDBTable(cls.__name__)
            alias = cls.sqlmeta.alias
            columns = [ColumnAS(cls.sqlmeta.idName, 'id')]
            aggregates = []
            for n,col in cls.sqlmeta.columns.iteritems():
                if isinstance(col.dbName, SQLCall):
                    aggregates.append(ColumnAS(col.dbName, n))
                else:
                    columns.append(ColumnAS(col.dbName, n))
            
            metajoin   = getattr(cls.sqlmeta, 'join', NoDefault)
            clause = getattr(cls.sqlmeta, 'clause', NoDefault)
            select = Select(columns,
                            distinct=True,
                            distinctOn=cls.sqlmeta.idName,
                            join=metajoin,
                            clause=clause)
            
            if aggregates:
                join = []
                last_alias = "%s_base" % alias
                last_id = "id"
                last = Alias(select, last_alias)
                columns = [SQLConstant("%s.%s"%(last_alias,x.expr2)) for x in columns]
                
                for i, agg in enumerate(aggregates):
                    agg_alias = "%s_%s" % (alias, i)
                    agg_id = '%s_id'%agg_alias
                    if not last.q.alias.endswith('base'):
                        last = None
                    new_alias = Alias(
                                             Select([ColumnAS(cls.sqlmeta.idName, agg_id), agg],
                                                    groupBy=cls.sqlmeta.idName,
                                                    join=metajoin,
                                                    clause=clause),
                                       agg_alias)
                    agg_join = LEFTJOINOn(last,
                                       new_alias,
                                       "%s.%s = %s.%s" % (last_alias, last_id, agg_alias, agg_id))
                    
                    join.append(agg_join)
                    columns.append(SQLConstant("%s.%s"%(agg_alias, agg.expr2)))
                    
                    last = new_alias
                    last_alias = agg_alias
                    last_id = agg_id
                select = Select(columns,
                                join=join)
            
            cls.sqlmeta.table = Alias(select, alias)
            cls.q = ViewSQLObjectTable(cls)
            for n,col in cls.sqlmeta.columns.iteritems():
                col.dbName = getattr(cls.q, n)


######
