from sqlobject.dbconnection import DBAPI
from sqlobject import col
cx_Oracle = None

import array

class OracleConnection(DBAPI):

    supportTransactions = True
    dbName = 'oracle'
    schemes = [dbName]

    def __init__(self, db, user, passwd='', host='localhost', port=1521, **kw):
        global cx_Oracle
        if cx_Oracle is None:
            import cx_Oracle
        self.module = cx_Oracle
        self.db = db
        self.user = user
        self.password = passwd
        self.host = host
        self.port = port
        DBAPI.__init__(self, **kw)

    def connectionFromURI(cls, uri):
        user, password, host, port, path, args = cls._parseURI(uri)
        return cls(db=path.strip('/'), user=user or '', passwd=password or '', host=host or 'localhost', port=port or 1521, **args)
    connectionFromURI = classmethod(connectionFromURI)

    def _setAutoCommit(self, conn, auto):
        pass

    def makeConnection(self):
        return cx_Oracle.connect('%s/%s@%s:%s/%s' % (self.user, self.password, self.host, self.port, self.db))

    def _queryInsertID(self, conn, soInstance, id, names, values):
        """Oracle can use 'sequences' to create new ids for a table.
        The sequence is created when the table is created.  """
        table = soInstance.sqlmeta.table
        idName = soInstance.sqlmeta.idName
        # Nothing ever sets _idSequence (why?) so this getattr call always returns the default name.
        sequenceName = getattr(soInstance, '_idSequence', '%s_ID_SEQ' % table)

        cs = conn.cursor()
        if id is None:
            # The select syntax requires a table name.  The Oracle convention is to select
            # from the dual table since it always exists.  It could be any table.
            print '***************************'
            print 'sequenceName: %s' % sequenceName
            print '***************************'
            print ''
            cs.execute('SELECT %s.NEXTVAL from dual' % sequenceName)
            id = cs.fetchone()[0]
        names = [idName] + names
        values = [id] + values
        q = self._insertSQL(table, names, values)
        if self.debug:
            self.printDebug(conn, q, 'QueryIns')
        print q
        cs.execute(q)
        if self.debugOutput:
            self.printDebug(conn, id, 'QueryIns', 'result')
        return id

    def _queryAddLimitOffset(self, query, start, end):
        # XXX Oracle doesn't support LIMIT
        return query

    def createTable(self, soClass):
        self.query('CREATE TABLE %s (\n%s\n)' % (soClass.sqlmeta.table, self.createColumns(soClass)))
        self.query("CREATE SEQUENCE %s_ID_SEQ" % soClass.sqlmeta.table)

    def createColumn(self, soClass, col):
        return col.oracleCreateSQL()

    def createReferenceConstraint(self, soClass, col):
        return col.oracleCreateReferenceConstraint()

    def createIndexSQL(self, soClass, index):
        return index.oracleCreateIndexSQL(soClass)

    def createIDColumn(self, soClass):
        key_type = {int: "INT", str: "VARCHAR(1000)"}[soClass.sqlmeta.idType]
        return '%s %s PRIMARY KEY' % (soClass.sqlmeta.idName, key_type)

    def createBinary(self, value):
        """Translate value to Oracle hex format, that is 01, 02, ... FF.
        This should work up to 4000 characters, the Oracle limit for strings
        in sql statements."""
        if value is None:
            return value
        else:
            # e.g. 'abc' --> '616263'
            # s = '%s%s%s' % (chr(253), chr(254), chr(255)) --> 'FDFEFF'

            x = ''.join(['%02X' % ord(c) for c in value])
            return x
    
    def returnBinary(self, value):
        """Translate value from Oracle hex format to a string of byte values."""
        try:
            len(value)
        except TypeError:
            return value
        a = array.array('B')
        l = []
        for i in range(len(value)-1):
            if i % 2:
                continue
            l.append(eval('0x%s%s' % (value[i], value[i+1])))
        a.fromlist(l)
        return a.tostring()
    
    def joinSQLType(self, join):
        return 'INT NOT NULL'

    def tableExists(self, tableName):
        for (table,) in self.queryAll("SELECT TABLE_NAME FROM USER_TABLES"):
            if table.lower() == tableName.lower():
                return True
        return False

    def addColumn(self, tableName, column):
        self.query('ALTER TABLE %s ADD %s' % (tableName, column.oracleCreateSQL()))

    def dropTable(self, tableName, cascade=False):
        self.query("DROP TABLE %s %s" % (tableName, cascade and 'CASCADE CONSTRAINTS' or ''))
        try:
            self.query("DROP SEQUENCE %s_ID_SEQ" % tableName)
        except:
            print 'Failed to drop seq %s_ID_SEQ.' % tableName
            pass

    def delColumn(self, tableName, column):
        self.query('ALTER TABLE %s DROP COLUMN %s' % (tableName, column.dbName))

    def columnsFromSchema(self, tableName, soClass):
        colData = self.queryAll("SELECT LOWER(COLUMN_NAME), DATA_TYPE, NULLABLE, DATA_DEFAULT, DATA_TYPE FROM USER_TAB_COLUMNS WHERE TABLE_NAME = UPPER('%s')" % tableName)
        primaryKey = self.queryOne("select lower(a.column_name) from sys.user_cons_columns a, sys.user_constraints b where b.table_name = '%s' and b.constraint_type = 'P' and b.constraint_name = a.constraint_name" % str.upper(tableName))

        results = []
        for field, t, nullAllowed, default, defaultType in colData:
            if primaryKey and field == primaryKey[0]:
                continue
            colClass, kw = self.guessClass(t)
            kw['name'] = soClass.sqlmeta.style.dbColumnToPythonAttr(field)
            kw['notNone'] = not nullAllowed
            if default:
                kw['default'] = self.decodeLong(default, defaultType)
            else:
                kw['default'] = None
            results.append(colClass(**kw))
        return results

    def decodeLong(self, value, type):
        if type == 'CHAR':
            return value.replace("'", "").strip()
        return value

    def guessClass(self, t):
        if t.startswith('int'):
            return col.IntCol, {}
        elif t.startswith('varchar'):
            return col.StringCol, {'length': int(t[8:-1])}
        elif t.startswith('char'):
            return col.StringCol, {'length': int(t[5:-1]), 'varchar': False}
        elif t.startswith('datetime'):
            return col.DateTimeCol, {}
        elif t.startswith('bool'):
            return col.BoolCol, {}
        elif t.startswith('tinyblob'):
            return col.BLOBCol, {"length": 2**8-1}
        elif t.startswith('tinytext'):
            return col.BLOBCol, {"length": 2**8-1, "varchar": True}
        elif t.startswith('blob'):
            return col.BLOBCol, {"length": 2**16-1}
        elif t.startswith('text'):
            return col.BLOBCol, {"length": 2**16-1, "varchar": True}
        elif t.startswith('mediumblob'):
            return col.BLOBCol, {"length": 2**24-1}
        elif t.startswith('mediumtext'):
            return col.BLOBCol, {"length": 2**24-1, "varchar": True}
        elif t.startswith('longblob'):
            return col.BLOBCol, {"length": 2**32}
        elif t.startswith('longtext'):
            return col.BLOBCol, {"length": 2**32, "varchar": True}
        else:
            return col.Col, {}

