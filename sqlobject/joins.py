import sqlbuilder
NoDefault = sqlbuilder.NoDefault
import styles
import classregistry
from col import popKey

__all__ = ['MultipleJoin', 'SQLMultipleJoin', 'RelatedJoin', 'SQLRelatedJoin', 'SingleJoin']

def getID(obj):
    try:
        return obj.id
    except AttributeError:
        return int(obj)

class Join(object):

    def __init__(self, otherClass=None, **kw):
        kw['otherClass'] = otherClass
        self.kw = kw
        if self.kw.has_key('joinMethodName'):
            self._joinMethodName = popKey(self.kw, 'joinMethodName')
        else:
            self._joinMethodName = None

    def _set_joinMethodName(self, value):
        assert self._joinMethodName == value or self._joinMethodName is None, "You have already given an explicit joinMethodName (%s), and you are now setting it to %s" % (self._joinMethodName, value)
        self._joinMethodName = value

    def _get_joinMethodName(self):
        return self._joinMethodName

    joinMethodName = property(_get_joinMethodName, _set_joinMethodName)
    name = joinMethodName

    def withClass(self, soClass):
        if self.kw.has_key('joinMethodName'):
            self._joinMethodName = self.kw['joinMethodName']
            del self.kw['joinMethodName']
        return self.baseClass(soClass=soClass,
                              joinDef=self,
                              joinMethodName=self._joinMethodName,
                              **self.kw)

# A join is separate from a foreign key, i.e., it is
# many-to-many, or one-to-many where the *other* class
# has the foreign key.
class SOJoin(object):

    def __init__(self,
                 soClass=None,
                 otherClass=None,
                 joinColumn=None,
                 joinMethodName=None,
                 orderBy=NoDefault,
                 joinDef=None):
        self.soClass = soClass
        self.joinDef = joinDef
        self.otherClassName = otherClass
        classregistry.registry(soClass.sqlmeta.registry).addClassCallback(
            otherClass, self._setOtherClass)
        self.joinColumn = joinColumn
        self.joinMethodName = joinMethodName
        self.orderBy = orderBy
        if not self.joinColumn:
            # Here we set up the basic join, which is
            # one-to-many, where the other class points to
            # us.
            self.joinColumn = styles.getStyle(
                self.soClass).tableReference(self.soClass.sqlmeta.table)

    def _setOtherClass(self, cls):
        self.otherClass = cls

    def hasIntermediateTable(self):
        return False

    def _applyOrderBy(self, results, defaultSortClass):
        if self.orderBy is NoDefault:
            self.orderBy = defaultSortClass.sqlmeta.defaultOrder
        if self.orderBy is not None:
            results.sort(sorter(self.orderBy))
        return results

def sorter(orderBy):
    if isinstance(orderBy, tuple) or isinstance(orderBy, list):
        if len(orderBy) == 1:
            orderBy = orderBy[0]
        else:
            fhead = sorter(orderBy[0])
            frest = sorter(orderBy[1:])
            return lambda a, b, fhead=fhead, frest=frest: fhead(a, b) or frest(a, b)
    if isinstance(orderBy, sqlbuilder.DESC) \
       and isinstance(orderBy.expr, sqlbuilder.SQLObjectField):
        orderBy = '-' + orderBy.expr.original
    elif isinstance(orderBy, sqlbuilder.SQLObjectField):
        orderBy = orderBy.original
    # @@: but we don't handle more complex expressions for orderings
    if orderBy.startswith('-'):
        orderBy = orderBy[1:]
        reverse = True
    else:
        reverse = False

    def cmper(a, b, attr=orderBy, rev=reverse):
        a = getattr(a, attr)
        b = getattr(b, attr)
        if rev:
            a, b = b, a
        if a is None:
            if b is None:
                return 0
            return -1
        if b is None:
            return 1
        return cmp(a, b)
    return cmper

# This is a one-to-many
class SOMultipleJoin(SOJoin):

    def __init__(self, addRemoveName=None, **kw):
        # addRemovePrefix is something like @@
        SOJoin.__init__(self, **kw)

        # Here we generate the method names
        if not self.joinMethodName:
            name = self.otherClassName[0].lower() + self.otherClassName[1:]
            if name.endswith('s'):
                name = name + "es"
            else:
                name = name + "s"
            self.joinMethodName = name
        if not addRemoveName:
            self.addRemoveName = capitalize(self.otherClassName)
        else:
            self.addRemoveName = addRemoveName

    def performJoin(self, inst):
        ids = inst._connection._SO_selectJoin(
            self.otherClass,
            self.joinColumn,
            inst.id)
        if inst.sqlmeta._perConnection:
            conn = inst._connection
        else:
            conn = None
        return self._applyOrderBy([self.otherClass.get(id, conn) for (id,) in ids if id is not None], self.otherClass)

    def _dbNameToPythonName(self):
        for column in self.otherClass.sqlmeta.columns.values():
            if column.dbName == self.joinColumn:
                return column.name
        return self.soClass.sqlmeta.style.dbColumnToPythonAttr(self.joinColumn)

class MultipleJoin(Join):
    baseClass = SOMultipleJoin

class SOSQLMultipleJoin(SOMultipleJoin):

    def performJoin(self, inst):
        if inst.sqlmeta._perConnection:
            conn = inst._connection
        else:
            conn = None
        pythonColumn = self._dbNameToPythonName()
        results = self.otherClass.select(getattr(self.otherClass.q, pythonColumn) == inst.id, connection=conn)
        if self.orderBy is NoDefault:
            self.orderBy = self.otherClass.sqlmeta.defaultOrder
        return results.orderBy(self.orderBy)

class SQLMultipleJoin(Join):
    baseClass = SOSQLMultipleJoin

# This is a many-to-many join, with an intermediary table
class SORelatedJoin(SOMultipleJoin):

    def __init__(self,
                 otherColumn=None,
                 intermediateTable=None,
                 createRelatedTable=True,
                 **kw):
        self.intermediateTable = intermediateTable
        self.otherColumn = otherColumn
        self.createRelatedTable = createRelatedTable
        SOMultipleJoin.__init__(self, **kw)
        classregistry.registry(
            self.soClass.sqlmeta.registry).addClassCallback(
            self.otherClassName, self._setOtherRelatedClass)

    def _setOtherRelatedClass(self, otherClass):
        if not self.intermediateTable:
            names = [self.soClass.sqlmeta.table,
                     otherClass.sqlmeta.table]
            names.sort()
            self.intermediateTable = '%s_%s' % (names[0], names[1])
        if not self.otherColumn:
            self.otherColumn = self.soClass.sqlmeta.style.tableReference(
                otherClass.sqlmeta.table)


    def hasIntermediateTable(self):
        return True

    def performJoin(self, inst):
        ids = inst._connection._SO_intermediateJoin(
            self.intermediateTable,
            self.otherColumn,
            self.joinColumn,
            inst.id)
        if inst.sqlmeta._perConnection:
            conn = inst._connection
        else:
            conn = None
        return self._applyOrderBy([self.otherClass.get(id, conn) for (id,) in ids if id is not None], self.otherClass)

    def remove(self, inst, other):
        inst._connection._SO_intermediateDelete(
            self.intermediateTable,
            self.joinColumn,
            getID(inst),
            self.otherColumn,
            getID(other))

    def add(self, inst, other):
        inst._connection._SO_intermediateInsert(
            self.intermediateTable,
            self.joinColumn,
            getID(inst),
            self.otherColumn,
            getID(other))

class RelatedJoin(MultipleJoin):
    baseClass = SORelatedJoin

# helper classes to SQLRelatedJoin
class OtherTableToJoin(sqlbuilder.SQLExpression):
    def __init__(self, otherTable, otherIdName, interTable, joinColumn):
        self.otherTable = otherTable
        self.otherIdName = otherIdName
        self.interTable = interTable
        self.joinColumn = joinColumn

    def __sqlrepr__(self, db):
        return '%s.%s = %s.%s' % (self.otherTable, self.otherIdName, self.interTable, self.joinColumn)

class JoinToTable(sqlbuilder.SQLExpression):
    def __init__(self, table, idName, interTable, joinColumn):
        self.table = table
        self.idName = idName
        self.interTable = interTable
        self.joinColumn = joinColumn

    def __sqlrepr__(self, db):
        return '%s.%s = %s.%s' % (self.interTable, self.joinColumn, self.table, self.idName)

class TableToId(sqlbuilder.SQLExpression):
    def __init__(self, table, idName, idValue):
        self.table = table
        self.idName = idName
        self.idValue = idValue

    def __sqlrepr__(self, db):
        return '%s.%s = %s' % (self.table, self.idName, self.idValue)

class SOSQLRelatedJoin(SORelatedJoin):
    def performJoin(self, inst):
        if inst.sqlmeta._perConnection:
            conn = inst._connection
        else:
            conn = None
        results = self.otherClass.select(sqlbuilder.AND(
            OtherTableToJoin(
                self.otherClass.sqlmeta.table, self.otherClass.sqlmeta.idName,
                self.intermediateTable, self.otherColumn
            ),
            JoinToTable(
                self.soClass.sqlmeta.table, self.soClass.sqlmeta.idName,
                self.intermediateTable, self.joinColumn
            ),
            TableToId(self.soClass.sqlmeta.table, self.soClass.sqlmeta.idName, inst.id),
        ), clauseTables=(self.soClass.sqlmeta.table, self.otherClass.sqlmeta.table, self.intermediateTable),
        connection=conn)
        if self.orderBy is NoDefault:
            self.orderBy = self.otherClass.sqlmeta.defaultOrder
        return results.orderBy(self.orderBy)

class SQLRelatedJoin(RelatedJoin):
    baseClass = SOSQLRelatedJoin

def capitalize(name):
    return name[0].capitalize() + name[1:]

class SOSingleJoin(SOMultipleJoin):

    def __init__(self, **kw):
        self.makeDefault = popKey(kw, 'makeDefault', False)
        SOMultipleJoin.__init__(self, **kw)

    def performJoin(self, inst):
        if inst.sqlmeta._perConnection:
            conn = inst._connection
        else:
            conn = None
        pythonColumn = self._dbNameToPythonName()
        results = self.otherClass.select(
            getattr(self.otherClass.q, pythonColumn) == inst.id,
            connection=conn
        )
        if results.count() == 0:
            if not self.makeDefault:
                return None
            else:
                kw = {pythonColumn[:-2]: inst} # skipping the ID (from foreignkeyID)
                return self.otherClass(**kw) # instanciating the otherClass with all
                # values to their defaults, except the foreign key
                # TODO I don't think this is the best way to know the column as foreignKey
                # reather than foreignKeyID, but I don't found a sqlmeta.style function
                # to do the work, if there isn't such function, I must create it.
        else:
            return results[0]

class SingleJoin(Join):
    baseClass = SOSingleJoin
