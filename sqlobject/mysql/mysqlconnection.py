from sqlobject.dbconnection import DBAPI
from sqlobject import col
MySQLdb = None

class MySQLConnection(DBAPI):

    supportTransactions = False
    dbName = 'mysql'
    schemes = [dbName]

    def __init__(self, db, user, passwd='', host='localhost', port=None, **kw):
        global MySQLdb
        if MySQLdb is None:
            import MySQLdb, MySQLdb.constants.CR, MySQLdb.constants.ER
        self.module = MySQLdb
        self.host = host
        self.port = port
        self.db = db
        self.user = user
        self.password = passwd
        self.kw = {}
        if MySQLdb.version_info[:3] >= (1, 2, 1):
            self.need_unicode = True
        else:
            self.need_unicode = False
        for key in ("unix_socket", "init_command",
                "read_default_file", "read_default_group"):
            if key in kw:
                self.kw[key] = col.popKey(kw, key)
        for key in ("connect_timeout", "compress", "named_pipe", "use_unicode",
                "client_flag", "local_infile"):
            if key in kw:
                self.kw[key] = int(col.popKey(kw, key))
        if "charset" in kw:
            self.dbEncoding = self.kw["charset"] = col.popKey(kw, "charset")
        else:
            self.dbEncoding = None
        if "sqlobject_encoding" in kw:
            self.encoding = col.popKey(kw, "sqlobject_encoding")
        else:
            self.encoding = 'ascii'
        DBAPI.__init__(self, **kw)

    def connectionFromURI(cls, uri):
        user, password, host, port, path, args = cls._parseURI(uri)
        return cls(db=path.strip('/'), user=user or '', passwd=password or '',
                   host=host or 'localhost', port=port or 0, **args)
    connectionFromURI = classmethod(connectionFromURI)

    def makeConnection(self):
        dbEncoding = self.dbEncoding
        if dbEncoding:
            from MySQLdb.connections import Connection
            if not hasattr(Connection, 'set_character_set'):
                # monkeypatch pre MySQLdb 1.2.1
                def character_set_name(self):
                    return dbEncoding + '_' + dbEncoding
                Connection.character_set_name = character_set_name
        try:
            conn = self.module.connect(host=self.host, port=self.port,
                db=self.db, user=self.user, passwd=self.password, **self.kw)
            if MySQLdb.version_info[:3] >= (1, 2, 2):
                conn.ping(True) # Attempt to reconnect. This setting is persistent.
        except self.module.OperationalError, e:
            raise self.module.OperationalError(
                "%s; used connection string: host=%s, port=%s, db=%s, user=%s, pwd=%s" % (
                e, self.host, self.port, self.db, self.user, self.password)
            )

        if hasattr(conn, 'autocommit'):
            conn.autocommit(bool(self.autoCommit))

        if self.dbEncoding:
            if hasattr(conn, 'set_character_set'): # MySQLdb 1.2.1 and later
                conn.set_character_set(self.dbEncoding)
            else: # pre MySQLdb 1.2.1
                # works along with monkeypatching code above
                conn.query("SET NAMES %s" % self.dbEncoding)

        return conn

    def _setAutoCommit(self, conn, auto):
        if hasattr(conn, 'autocommit'):
            conn.autocommit(auto)

    def _executeRetry(self, conn, cursor, query):
        while 1:
            try:
                # For MySQLdb 1.2.1 and later, we go
                # encoding->unicode->charset (in the mysql db)
                if self.need_unicode and not isinstance(query, unicode):
                    query = unicode(query, self.encoding)
                return cursor.execute(query)
            except MySQLdb.OperationalError, e:
                if e.args[0] == 2013: # SERVER_LOST error
                    if self.debug:
                        self.printDebug(conn, str(e), 'ERROR')
                else:
                    raise

    def _queryInsertID(self, conn, soInstance, id, names, values):
        table = soInstance.sqlmeta.table
        idName = soInstance.sqlmeta.idName
        c = conn.cursor()
        if id is not None:
            names = [idName] + names
            values = [id] + values
        q = self._insertSQL(table, names, values)
        if self.debug:
            self.printDebug(conn, q, 'QueryIns')
        self._executeRetry(conn, c, q)
        if id is None:
            try:
                id = c.lastrowid
            except AttributeError:
                id = c.insert_id()
        if self.debugOutput:
            self.printDebug(conn, id, 'QueryIns', 'result')
        return id

    def _queryAddLimitOffset(self, query, start, end):
        if not start:
            return "%s LIMIT %i" % (query, end)
        if not end:
            return "%s LIMIT %i, -1" % (query, start)
        return "%s LIMIT %i, %i" % (query, start, end-start)

    def createColumn(self, soClass, col):
        return col.mysqlCreateSQL()

    def createIndexSQL(self, soClass, index):
        return index.mysqlCreateIndexSQL(soClass)

    def createIDColumn(self, soClass):
        return '%s INT PRIMARY KEY AUTO_INCREMENT' % soClass.sqlmeta.idName

    def joinSQLType(self, join):
        return 'INT NOT NULL'

    def tableExists(self, tableName):
        try:
            # Use DESCRIBE instead of SHOW TABLES because SHOW TABLES
            # assumes there is a default database selected
            # which is not always True (for an embedded application, e.g.)
            self.query('DESCRIBE %s' % (tableName))
            return True
        except MySQLdb.ProgrammingError, e:
            if e.args[0] == 1146: # ER_NO_SUCH_TABLE
                return False
            raise

    def addColumn(self, tableName, column):
        self.query('ALTER TABLE %s ADD COLUMN %s' %
                   (tableName,
                    column.mysqlCreateSQL()))

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
            if self.kw.get('use_unicode') and colClass is col.StringCol:
                colClass = col.UnicodeCol
            kw['name'] = soClass.sqlmeta.style.dbColumnToPythonAttr(field)
            kw['dbName'] = field
            
            # Since MySQL 5.0, 'NO' is returned in the NULL column (SQLObject expected '')
            kw['notNone'] = (nullAllowed.upper() != 'YES' and True or False)
            
            if default and t.startswith('int'):
                kw['default'] = int(default)
            elif default and t.startswith('float'):
                kw['default'] = float(default)
            elif default == 'CURRENT_TIMESTAMP' and t == 'timestamp':
                kw['default'] = None
            elif default and colClass is col.BoolCol:
                kw['default'] = int(default) and True or False
            else:
                kw['default'] = default
            # @@ skip key...
            # @@ skip extra...
            results.append(colClass(**kw))
        return results

    def guessClass(self, t):
        if t.startswith('int'):
            return col.IntCol, {}
        elif t.startswith('enum'):
            values = []
            for i in t[5:-1].split(','): # take the enum() off and split
                values.append(i[1:-1]) # remove the surrounding \'
            return col.EnumCol, {'enumValues': values}
        elif t.startswith('double'):
            return col.FloatCol, {}
        elif t.startswith('varchar'):
            colType = col.StringCol
            if self.kw.get('use_unicode', False):
                colType = col.UnicodeCol
            if t.endswith('binary'):
                return colType, {'length': int(t[8:-8]),
                                       'char_binary': True}
            else:
                return colType, {'length': int(t[8:-1])}
        elif t.startswith('char'):
            if t.endswith('binary'):
                return col.StringCol, {'length': int(t[5:-8]),
                                       'varchar': False,
                                       'char_binary': True}
            else:
                return col.StringCol, {'length': int(t[5:-1]),
                                       'varchar': False}
        elif t.startswith('datetime'):
            return col.DateTimeCol, {}
        elif t.startswith('bool'):
            return col.BoolCol, {}
        elif t.startswith('tinyblob'):
            return col.BLOBCol, {"length": 2**8-1}
        elif t.startswith('tinytext'):
            return col.StringCol, {"length": 2**8-1, "varchar": True}
        elif t.startswith('blob'):
            return col.BLOBCol, {"length": 2**16-1}
        elif t.startswith('text'):
            return col.StringCol, {"length": 2**16-1, "varchar": True}
        elif t.startswith('mediumblob'):
            return col.BLOBCol, {"length": 2**24-1}
        elif t.startswith('mediumtext'):
            return col.StringCol, {"length": 2**24-1, "varchar": True}
        elif t.startswith('longblob'):
            return col.BLOBCol, {"length": 2**32}
        elif t.startswith('longtext'):
            return col.StringCol, {"length": 2**32, "varchar": True}
        else:
            return col.Col, {}
