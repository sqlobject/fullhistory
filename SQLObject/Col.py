"""
Col
"""

import SQLBuilder
import re
import Constraints
from include import Validator

NoDefault = SQLBuilder.NoDefault
True, False = 1==1, 0==1

########################################
## Columns
########################################

# Col is essentially a column definition, it doesn't have
# much logic to it.
class SOCol(object):

    def __init__(self,
                 name,
                 soClass,
                 dbName=None,
                 default=NoDefault,
                 foreignKey=None,
                 alternateID=False,
                 alternateMethodName=None,
                 constraints=None,
                 notNull=NoDefault,
                 notNone=NoDefault,
                 unique=NoDefault,
                 sqlType=None,
                 columnDef=None,
                 validator=None,
                 immutable=False,
                 cascade=None):

        # This isn't strictly true, since we *could* use backquotes or
        # " or something (database-specific) around column names, but
        # why would anyone *want* to use a name like that?
        # @@: I suppose we could actually add backquotes to the
        # dbName if we needed to...
        assert SQLBuilder.sqlIdentifier(name), 'Name must be SQL-safe (letters, numbers, underscores): %s' \
               % repr(name)
        assert name != 'id', 'The column name "id" is reserved for SQLObject use (and is implicitly created).'
        assert name, "You must provide a name for all columns"

        self.columnDef = columnDef

        self.immutable = immutable

        # cascade can be one of:
        # None: no constraint is generated
        # True: a CASCADE constraint is generated
        # False: a RESTRICT constraint is generated
        self.cascade = cascade

        if type(constraints) not in (type([]), type(())):
            constraints = [constraints]
        self.constraints = self.autoConstraints() + constraints

        self.notNone = False
        if notNull is not NoDefault:
            self.notNone = notNull
            assert notNone is NoDefault or \
                   (not notNone) == (not notNull), \
                   "The notNull and notNone arguments are aliases, and must not conflict.  You gave notNull=%r, notNone=%r" % (notNull, notNone)
        elif notNone is not NoDefault:
            self.notNone = notNone
        if self.notNone:
            self.constraints = [Constraints.notNull] + self.constraints

        self.name = name
        self.soClass = None
        self._default = default
        self.customSQLType = sqlType

        self.foreignKey = foreignKey
        if self.foreignKey:
            #assert self.name.upper().endswith('ID'), "All foreign key columns must end with 'ID' (%s)" % repr(self.name)
            if not self.name.upper().endswith('ID'):
                self.foreignName = self.name
                self.name = self.name + "ID"
            else:
                self.foreignName = self.name[:-2]
        else:
            self.foreignName = None

        # if they don't give us a specific database name for
        # the column, we separate the mixedCase into mixed_case
        # and assume that.
        if dbName is None:
            self.dbName = soClass._style.pythonAttrToDBColumn(self.name)
        else:
            self.dbName = dbName

        # alternateID means that this is a unique column that
        # can be used to identify rows
        self.alternateID = alternateID
        if self.alternateID and alternateMethodName is None:
            self.alternateMethodName = 'by' + self.name[0].capitalize() + self.name[1:]
        else:
            self.alternateMethodName = alternateMethodName

        if unique is NoDefault:
            self.unique = alternateID
        else:
            self.unique = unique

        self.validator = validator

    def _set_validator(self, value):
        self._validator = value
        if self._validator:
            self.toPython = self._validator.toPython
            self.fromPython = self._validator.fromPython
        else:
            self.toPython = None
            self.fromPython = None

    def _get_validator(self):
        return self._validator

    validator = property(_get_validator, _set_validator)

    def autoConstraints(self):
        return []

    def _get_default(self):
        # A default can be a callback or a plain value,
        # here we resolve the callback
        if self._default is NoDefault:
            return NoDefault
        elif hasattr(self._default, '__sqlrepr__'):
            return self._default
        elif callable(self._default):
            return self._default()
        else:
            return self._default
    default = property(_get_default, None, None)

    def _get_joinName(self):
        assert self.name[-2:] == 'ID'
        return self.name[:-2]
    joinName = property(_get_joinName, None, None)

    def __repr__(self):
        r = '<%s %s' % (self.__class__.__name__, self.name)
        if self.default is not NoDefault:
            r += ' default=%s' % repr(self.default)
        if self.foreignKey:
            r += ' connected to %s' % self.foreignKey
        if self.alternateID:
            r += ' alternate ID'
        if self.notNone:
            r += ' not null'
        return r + '>'

    def createSQL(self):
        return ' '.join([self._sqlType() + self._extraSQL()])

    def _extraSQL(self):
        result = []
        if self.notNone or self.alternateID:
            result.append('NOT NULL')
        if self.unique or self.alternateID:
            result.append('UNIQUE')
        return result

    def _sqlType(self):
        if self.customSQLType is None:
            raise ValueError, ("Col %s (%s) cannot be used for automatic "
                               "schema creation (too abstract)" %
                               (self.name, self.__class__))
        else:
            return self.customSQLType

    def _mysqlType(self):
        return self._sqlType()

    def _postgresType(self):
        return self._sqlType()

    def _sqliteType(self):
        # SQLite is naturally typeless, so as a fallback it uses
        # no type.
        try:
            return self._sqlType()
        except ValueError:
            return ''

    def _sybaseType(self):
        return self._sqlType()

    def _firebirdType(self):
        return self._sqlType()

    def mysqlCreateSQL(self):
        return ' '.join([self.dbName, self._mysqlType()] + self._extraSQL())

    def postgresCreateSQL(self):
        return ' '.join([self.dbName, self._postgresType()] + self._extraSQL())

    def sqliteCreateSQL(self):
        return ' '.join([self.dbName, self._sqliteType()] + self._extraSQL())

    def sybaseCreateSQL(self):
        return ' '.join([self.dbName, self._sybaseType()] + self._extraSQL())

    def firebirdCreateSQL(self):
        # Ian Sparks pointed out that fb is picky about the order
        # of the NOT NULL clause in a create statement.  So, we handle
        # them differently for Enum columns.
        if not isinstance(self, SOEnumCol):
            return ' '.join([self.dbName, self._firebirdType()] + self._extraSQL())
        else:
            return ' '.join([self.dbName] + self._extraSQL() + [self._firebirdType()])

class Col(object):

    baseClass = SOCol

    def __init__(self, name=None, **kw):
        kw['name'] = name
        kw['columnDef'] = self
        self.kw = kw

    def setName(self, value):
        assert self.kw['name'] is None, "You cannot change a name after it has already been set (from %s to %s)" % (self.kw['name'], value)
        self.kw['name'] = value

    def withClass(self, soClass):
        return self.baseClass(soClass=soClass, **self.kw)

class SOStringCol(SOCol):

    # 3-03 @@: What about BLOB?

    def __init__(self, **kw):
        self.length = popKey(kw, 'length')
        self.varchar = popKey(kw, 'varchar', 'auto')
        if not self.length:
            assert self.varchar == 'auto' or not self.varchar, \
                   "Without a length strings are treated as TEXT, not varchar"
            self.varchar = False
        elif self.varchar == 'auto':
            self.varchar = True

        SOCol.__init__(self, **kw)

    def autoConstraints(self):
        constraints = [Constraints.isString]
        if self.length is not None:
            constraints += [Constraints.MaxLength(self.length)]
        return constraints

    def _sqlType(self):
        if not self.length:
            return 'TEXT'
        elif self.varchar:
            return 'VARCHAR(%i)' % self.length
        else:
            return 'CHAR(%i)' % self.length

    def _firebirdType(self):
        if not self.length:
            return 'BLOB SUB_TYPE TEXT'
        else:
            return self._sqlType()

class StringCol(Col):
    baseClass = SOStringCol

class SOIntCol(SOCol):

    # 3-03 @@: support precision, maybe max and min directly

    def autoConstraints(self):
        return [Constraints.isInt]

    def _sqlType(self):
        return 'INT'

class IntCol(Col):
    baseClass = SOIntCol

class BoolValidator(Validator.Validator):

    def fromPython(self, value, state):
        if value:
            return SQLBuilder.TRUE
        else:
            return SQLBuilder.FALSE

class SOBoolCol(SOCol):

    def __init__(self, **kw):
        SOCol.__init__(self, **kw)
        self.validator = Validator.All.join(BoolValidator(), self.validator)

    def autoConstraints(self):
        return [Constraints.isBool]

    def _postgresType(self):
        return 'BOOL'

    def _mysqlType(self):
        return "TINYINT"

    def _sybaseType(self):
        return "BIT"

class BoolCol(Col):
    baseClass = SOBoolCol

class SOFloatCol(SOCol):

    # 3-03 @@: support precision (e.g., DECIMAL)

    def autoConstraints(self):
        return [Constraints.isFloat]

    def _sqlType(self):
        return 'FLOAT'

class FloatCol(Col):
    baseClass = SOFloatCol

class SOKeyCol(SOCol):

    # 3-03 @@: this should have a simplified constructor
    # Should provide foreign key information for other DBs.

    def _mysqlType(self):
        return 'INT'

    def _sqliteType(self):
        return 'INT'

    def _postgresType(self):
        return 'INT'

    def _sybaseType(self):
        return 'INT'

    def _firebirdType(self):
        return 'INT'

class KeyCol(Col):

    baseClass = SOKeyCol

class SOForeignKey(SOKeyCol):

    def __init__(self, **kw):
        foreignKey = kw['foreignKey']
        style = kw['soClass']._style
        if not kw.get('name'):
            kw['name'] = style.instanceAttrToIDAttr(style.pythonClassToAttr(foreignKey))
        else:
            if not kw['name'].upper().endswith('ID'):
                kw['name'] = style.instanceAttrToIDAttr(kw['name'])
        SOKeyCol.__init__(self, **kw)

    def postgresCreateSQL(self):
        from SQLObject import findClass
        sql = SOKeyCol.postgresCreateSQL(self)
        if self.cascade is not None:
            other = findClass(self.foreignKey)
            tName = other._table
            idName = other._idName
            action = self.cascade and 'CASCADE' or 'RESTRICT'
            constraint = ('CONSTRAINT %(tName)s_exists '
                          'FOREIGN KEY(%(colName)s) '
                          'REFERENCES %(tName)s(%(idName)s) '
                          'ON DELETE %(action)s' %
                          {'tName':tName,
                           'colName':self.dbName,
                           'idName':idName,
                           'action':action})
            sql = ', '.join([sql, constraint])
        return sql

    def sybaseCreateSQL(self):
        from SQLObject import findClass
        sql = SOKeyCol.sybaseCreateSQL(self)
        other = findClass(self.foreignKey)
        tName = other._table
        idName = other._idName
        reference = ('REFERENCES %(tName)s(%(idName)s) ' %
                     {'tName':tName,
                      'idName':idName})
        sql = ' '.join([sql, reference])
        return sql

class ForeignKey(KeyCol):

    baseClass = SOForeignKey

    def __init__(self, foreignKey=None, **kw):
        KeyCol.__init__(self, foreignKey=foreignKey, **kw)

class SOEnumCol(SOCol):

    def __init__(self, **kw):
        self.enumValues = popKey(kw, 'enumValues', None)
        assert self.enumValues is not None, \
               'You must provide an enumValues keyword argument'
        SOCol.__init__(self, **kw)

    def autoConstraints(self):
        return [Constraints.isString, Constraints.InList(self.enumValues)]

    def _mysqlType(self):
        return "ENUM(%s)" % ', '.join([SQLBuilder.sqlrepr(v, 'mysql') for v in self.enumValues])

    def _postgresType(self):
        length = max(map(len, self.enumValues))
        enumValues = ', '.join([SQLBuilder.sqlrepr(v, 'postgres') for v in self.enumValues])
        checkConstraint = "CHECK (%s in (%s))" % (self.dbName, enumValues)
        return "VARCHAR(%i) %s" % (length, checkConstraint)

    def _sqliteType(self):
        return self._postgresType()

    def _sybaseType(self):
        return self._postgresType()

    def _firebirdType(self):
        return self._postgresType()

class EnumCol(Col):
    baseClass = SOEnumCol

class SODateTimeCol(SOCol):

    # 3-03 @@: provide constraints; right now we let the database
    # do any parsing and checking.  And DATE and TIME?

    def _mysqlType(self):
        return 'DATETIME'

    def _postgresType(self):
        return 'TIMESTAMP'

    def _sybaseType(self):
        return 'DATETIME'

class DateTimeCol(Col):
    baseClass = SODateTimeCol

class SODateCol(SOCol):

    # 3-03 @@: provide constraints; right now we let the database
    # do any parsing and checking.  And DATE and TIME?

    def _mysqlType(self):
        return 'DATE'

    def _postgresType(self):
        return 'DATE'

    def _sybaseType(self):
        return self._postgresType()

class DateCol(Col):
    baseClass = SODateCol

class SODecimalCol(SOCol):

    def __init__(self, **kw):
        self.size = popKey(kw, 'size', NoDefault)
        assert self.size is not NoDefault, \
               "You must give a size argument"
        self.precision = popKey(kw, 'precision', NoDefault)
        assert self.precision is not NoDefault, \
               "You must give a precision argument"
        SOCol.__init__(self, **kw)

    def _sqlType(self):
        return 'DECIMAL(%i, %i)' % (self.size, self.precision)

class DecimalCol(Col):
    baseClass = SODecimalCol

class SOCurrencyCol(SODecimalCol):

    def __init__(self, **kw):
        pushKey(kw, 'size', 10)
        pushKey(kw, 'precision', 2)
        SODecimalCol.__init__(self, **kw)

class CurrencyCol(DecimalCol):
    baseClass = SOCurrencyCol

def popKey(kw, name, default=None):
    if not kw.has_key(name):
        return default
    value = kw[name]
    del kw[name]
    return value

def pushKey(kw, name, value):
    if not kw.has_key(name):
        kw[name] = value

all = []
for key, value in globals().items():
    if isinstance(value, type) and issubclass(value, Col):
        all.append(key)
__all__ = all
