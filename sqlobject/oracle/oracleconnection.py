from sqlobject.dbconnection import DBAPI
from sqlobject import col
cx_Oracle = None
## DCOracle2 = None

import array

class OracleConnection(DBAPI):

    supportTransactions = True
    dbName = 'oracle'
    schemes = [dbName]

    def __init__(self, db, user, passwd='', host='localhost', **kw):
        global cx_Oracle
##         global DCOracle2
        if cx_Oracle is None:
            import cx_Oracle
        self.module = cx_Oracle
##         if DCOracle2 is None:
##             import DCOracle2
##         self.module = DCOracle2
        self.host = host
        self.db = db
        self.user = user
        self.password = passwd
        DBAPI.__init__(self, **kw)

    def connectionFromURI(cls, uri):
        user, password, host, path, args = cls._parseURI(uri)
        return cls(db=path.strip('/'), user=user or '', passwd=password or '',
                   host=host or 'localhost', **args)
    connectionFromURI = classmethod(connectionFromURI)

    def makeConnection(self):
        return cx_Oracle.connect('%s/%s@%s' % (self.user, self.password, self.db))
##         return DCOracle2.connect('%s/%s@%s' % (self.user, self.password, self.db))
##         return DCOracle2.connect(host=self.host, db=self.db,
##                                  user=self.user, passwd=self.password)

    def _queryInsertID(self, conn, soInstance, id, names, values):
        """Oracle can use 'sequences' to create new ids for a table.
        The sequence is created when the table is created.  """
        table = soInstance._table
        idName = soInstance._idName
        # Nothing ever sets _idSequence (why?) so this getattr call always returns the default name.
        sequenceName = getattr(soInstance, '_idSequence', '%s_ID_SEQ' % table)
        if id is None:
            # The select syntax requires a table name.  The Oracle convention is to select
            # from the dual table since it always exists.  It could be any table.
            row = self.queryOne('SELECT %s.NEXTVAL from dual' % sequenceName)
            id = row[0]
        names = [idName] + names
        values = [id] + values
        q = self._insertSQL(table, names, values)
        if self.debug:
            self.printDebug(conn, q, 'QueryIns')
        self.query(q)
        if self.debugOutput:
            self.printDebug(conn, id, 'QueryIns', 'result')
        return id

    def _queryAddLimitOffset(self, query, start, end):
        # XXX Oracle doesn't support LIMIT
        return query

    def createTable(self, soClass):
        self.query('CREATE TABLE %s (\n%s\n)' % \
                   (soClass._table, self.createColumns(soClass)))
        self.query("CREATE SEQUENCE %s_ID_SEQ" % soClass._table)

    def createColumn(self, soClass, col):
        return col.oracleCreateSQL()

    def createIndexSQL(self, soClass, index):
        return index.oracleCreateIndexSQL(soClass)

    def createIDColumn(self, soClass):
        return '%s INT PRIMARY KEY' % soClass._idName

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
        self.query('ALTER TABLE %s ADD COLUMN %s' %
                   (tableName,
                    column.oracleCreateSQL()))

    def dropTable(self, tableName, cascade=False):
        self.query("DROP TABLE %s %s" % (tableName,
                                         cascade and 'CASCADE CONSTRAINTS' or ''))
##         self.query("DROP TABLE %s" % tableName)
        self.query("DROP SEQUENCE %s_ID_SEQ" % tableName)

    def delColumn(self, tableName, column):
        self.query('ALTER TABLE %s DROP COLUMN %s' %
                   (tableName,
                    column.dbName))

    def columnsFromSchema(self, tableName, soClass):
        colData = self.queryAll("SHOW COLUMNS FROM %s"
                                % tableName)
        results = []
        for field, t, nullAllowed, key, default, extra in colData:
            if field == 'id':
                continue
            colClass, kw = self.guessClass(t)
            kw['name'] = soClass._style.dbColumnToPythonAttr(field)
            kw['notNone'] = not nullAllowed
            kw['default'] = default
            # @@ skip key...
            # @@ skip extra...
            results.append(colClass(**kw))
        return results

    def guessClass(self, t):
        if t.startswith('int'):
            return col.IntCol, {}
        elif t.startswith('varchar'):
            return col.StringCol, {'length': int(t[8:-1])}
        elif t.startswith('char'):
            return col.StringCol, {'length': int(t[5:-1]),
                                   'varchar': False}
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
        
