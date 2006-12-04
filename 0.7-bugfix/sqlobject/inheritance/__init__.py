from sqlobject import sqlbuilder
from sqlobject import classregistry
from sqlobject.main import sqlmeta, SQLObject, SelectResults, True, False, \
   makeProperties, getterName, setterName
import iteration


try:
    basestring
except NameError: # Python 2.2
    import types
    basestring = (types.StringType, types.UnicodeType)


class InheritableSelectResults(SelectResults):
    IterationClass = iteration.InheritableIteration

    def __init__(self, sourceClass, clause, clauseTables=None,
                 **ops):
        if clause is None or isinstance(clause, str) and clause == 'all':
            clause = sqlbuilder.SQLTrueClause
        tablesDict = sqlbuilder.tablesUsedDict(clause)
        tablesDict[sourceClass.sqlmeta.table] = 1
        orderBy = ops.get('orderBy')
        if orderBy and not isinstance(orderBy, basestring):
            tablesDict.update(sqlbuilder.tablesUsedDict(orderBy))
        #DSM: if this class has a parent, we need to link it
        #DSM: and be sure the parent is in the table list.
        #DSM: The following code is before clauseTables
        #DSM: because if the user uses clauseTables
        #DSM: (and normal string SELECT), he must know what he wants
        #DSM: and will do himself the relationship between classes.
        if type(clause) is not str:
            tableRegistry = {}
            allClasses = classregistry.registry(
                sourceClass.sqlmeta.registry).allClasses()
            for registryClass in allClasses:
                if registryClass.sqlmeta.table in tablesDict:
                    #DSM: By default, no parents are needed for the clauses
                    tableRegistry[registryClass] = registryClass
            tableRegistryCopy = tableRegistry.copy()
            for childClass in tableRegistryCopy:
                if childClass not in tableRegistry:
                    continue
                currentClass = childClass
                while currentClass:
                    if tableRegistryCopy.has_key(currentClass):
                        if currentClass in tableRegistry:
                            #DSM: Remove this class as it is a parent one
                            #DSM: of a needed children
                            del tableRegistry[currentClass]
                        #DSM: Must keep the last parent needed
                        #DSM: (to limit the number of join needed)
                        tableRegistry[childClass] = currentClass
                    currentClass = currentClass._parentClass
            #DSM: Table registry contains only the last children
            #DSM: or standalone classes
            parentClause = []
            for (currentClass, minParentClass) in tableRegistry.items():
                while currentClass != minParentClass and currentClass._parentClass:
                    parentClass = currentClass._parentClass
                    parentClause.append(currentClass.q.id == parentClass.q.id)
                    currentClass = parentClass
                    tablesDict[currentClass.sqlmeta.table] = 1
            clause = reduce(sqlbuilder.AND, parentClause, clause)

        super(InheritableSelectResults, self).__init__(sourceClass, clause, clauseTables,
                 **ops)


class InheritableSQLMeta(sqlmeta):
    def addColumn(sqlmeta, columnDef, changeSchema=False, connection=None, childUpdate=False):
        soClass = sqlmeta.soClass
        #DSM: Try to add parent properties to the current class
        #DSM: Only do this once if possible at object creation and once for
        #DSM: each new dynamic column to refresh the current class
        if childUpdate or soClass._parentClass:
            for col in soClass._parentClass.sqlmeta.columnList:
                cname = col.name
                if cname == 'childName': continue
                if cname.endswith("ID"): cname = cname[:-2]
                setattr(soClass, getterName(cname), eval(
                    'lambda self: self._parent.%s' % cname))
                if not col.immutable:
                    setattr(soClass, setterName(cname), eval(
                        'lambda self, val: setattr(self._parent, %s, val)'
                        % repr(cname)))
            if childUpdate:
                makeProperties(soClass)
                return

        if columnDef:
            super(InheritableSQLMeta, sqlmeta).addColumn(columnDef, changeSchema, connection)

        #DSM: Update each child class if needed and existing (only for new
        #DSM: dynamic column as no child classes exists at object creation)
        if columnDef and hasattr(soClass, "q"):
            q = getattr(soClass.q, columnDef.name, None)
        else:
            q = None
        for c in soClass._childClasses.values():
            c.sqlmeta.addColumn(columnDef, connection=connection, childUpdate=True)
            if q: setattr(c.q, columnDef.name, q)

    addColumn = classmethod(addColumn)

    def delColumn(sqlmeta, column, changeSchema=False, connection=None):
        soClass = sqlmeta.soClass
        super(InheritableSQLMeta, sqlmeta).delColumn(column, changeSchema, connection)

        if isinstance(column, str):
            name = column
        else:
            name = column.name

        #DSM: Update each child class if needed
        #DSM: and delete properties for this column
        for c in soClass._childClasses.values():
            delattr(c.q, name)

    delColumn = classmethod(delColumn)

    def addJoin(sqlmeta, joinDef, childUpdate=False):
        soClass = sqlmeta.soClass
        #DSM: Try to add parent properties to the current class
        #DSM: Only do this once if possible at object creation and once for
        #DSM: each new dynamic join to refresh the current class
        if childUpdate or soClass._parentClass:
            for join in soClass._parentClass.sqlmeta.joins:
                jname = join.joinMethodName
                jarn  = join.addRemoveName
                setattr(soClass, getterName(jname),
                    eval('lambda self: self._parent.%s' % jname))
                if hasattr(join, 'remove'):
                    setattr(soClass, 'remove' + jarn,
                        eval('lambda self,o: self._parent.remove%s(o)' % jarn))
                if hasattr(join, 'add'):
                    setattr(soClass, 'add' + jarn,
                        eval('lambda self,o: self._parent.add%s(o)' % jarn))
            if childUpdate:
                makeProperties(soClass)
                return

        if joinDef:
            super(InheritableSQLMeta, sqlmeta).addJoin(joinDef)

        #DSM: Update each child class if needed and existing (only for new
        #DSM: dynamic join as no child classes exists at object creation)
        for c in soClass._childClasses.values():
            c.sqlmeta.addJoin(joinDef, childUpdate=True)

    addJoin = classmethod(addJoin)

    def delJoin(sqlmeta, joinDef):
        soClass = sqlmeta.soClass
        super(InheritableSQLMeta, sqlmeta).delJoin(joinDef)

        #DSM: Update each child class if needed
        #DSM: and delete properties for this join
        for c in soClass._childClasses.values():
            delattr(c, meth)

    delJoin = classmethod(delJoin)


class InheritableSQLObject(SQLObject):

    sqlmeta = InheritableSQLMeta
    _inheritable = True
    SelectResultsClass = InheritableSelectResults

    def get(cls, id, connection=None, selectResults=None, childResults=None, childUpdate=False):

        val = super(InheritableSQLObject, cls).get(id, connection, selectResults)

        #DSM: If we are updating a child, we should never return a child...
        if childUpdate: return val
        #DSM: If this class has a child, return the child
        if 'childName' in cls.sqlmeta.columns:
             childName = val.childName
             if childName is not None:
                 return val._childClasses[childName].get(id, connection=connection, selectResults=childResults)
        #DSM: Now, we know we are alone or the last child in a family...
        #DSM: It's time to find our parents
        inst = val
        while inst._parentClass and not inst._parent:
            inst._parent = inst._parentClass.get(id, connection=connection, childUpdate=True)
            inst = inst._parent
        #DSM: We can now return ourself
        return val

    get = classmethod(get)

    def _notifyFinishClassCreation(cls):
        if not cls.sqlmeta.columnList:
            # There are no columns - call addColumn to propagate columns
            # from parent classes to children
            cls.sqlmeta.addColumn(None)
        if not cls.sqlmeta.joins:
            # There are no joins - call addJoin to propagate joins
            # from parent classes to children
            cls.sqlmeta.addJoin(None)
    _notifyFinishClassCreation = classmethod(_notifyFinishClassCreation)

    def _create(self, id, **kw):

        #DSM: If we were called by a children class,
        #DSM: we must retreive the properties dictionary.
        #DSM: Note: we can't use the ** call paremeter directly
        #DSM: as we must be able to delete items from the dictionary
        #DSM: (and our children must know that the items were removed!)
        if kw.has_key('kw'):
            kw = kw['kw']
        #DSM: If we are the children of an inheritable class,
        #DSM: we must first create our parent
        if self._parentClass:
            parentClass = self._parentClass
            new_kw = {}
            parent_kw = {}
            for (name, value) in kw.items():
                if hasattr(parentClass, name):
                    parent_kw[name] = value
                else:
                    new_kw[name] = value
            kw = new_kw

            # Need to check that we have enough data to sucesfully
            # create the current subclass otherwise we will leave
            # the database in an inconsistent state.
            for col in self.sqlmeta.columnList:
                if (col._default == sqlbuilder.NoDefault) and \
                        (col.name not in kw) and (col.foreignName not in kw):
                    raise TypeError, "%s() did not get expected keyword argument %s" % (self.__class__.__name__, col.name)

            self._parent = parentClass(kw=parent_kw, connection=self._connection)
            self._parent.childName = self.__class__.__name__

            id = self._parent.id

        super(InheritableSQLObject, self)._create(id, **kw)

    def _findAlternateID(cls, name, dbName, value, connection=None):
        result = list(cls.selectBy(connection, **{name: value}))
        if not result:
            return result, None
        obj = result[0]
        return [obj.id], obj
    _findAlternateID = classmethod(_findAlternateID)

    def selectBy(cls, connection=None, **kw):
        clause = []
        foreignColumns = {}
        currentClass = cls
        while currentClass:
            foreignColumns.update(dict([(column.foreignName, name)
                for (name, column) in currentClass.sqlmeta.columns.items()
                    if column.foreignKey
            ]))
            currentClass = currentClass._parentClass
        for name, value in kw.items():
            if name in foreignColumns:
                name = foreignColumns[name] # translate "key" to "keyID"
                if isinstance(value, SQLObject):
                    value = value.id
            currentClass = cls
            while currentClass:
                try:
                    clause.append(getattr(currentClass.q, name) == value)
                    break
                except AttributeError, err:
                    pass
                currentClass = currentClass._parentClass
            else:
                raise AttributeError("'%s' instance has no attribute '%s'"
                    % (cls.__name__, name))
        if clause:
            clause = reduce(sqlbuilder.AND, clause)
        else:
            clause = None # select all
        conn = connection or cls._connection
        return cls.SelectResultsClass(cls, clause, connection=conn)

    selectBy = classmethod(selectBy)

    def destroySelf(self):
        #DSM: If this object has parents, recursivly kill them
        if hasattr(self, '_parent') and self._parent:
            self._parent.destroySelf()
        super(InheritableSQLObject, self).destroySelf()

    def _reprItems(self):
        items = super(InheritableSQLObject, self)._reprItems()
        # add parent attributes (if any)
        if self._parentClass:
            items.extend(self._parent._reprItems())
        # filter out our special column
        return [item for item in items if item[0] != 'childName']

__all__ = ['InheritableSQLObject']
