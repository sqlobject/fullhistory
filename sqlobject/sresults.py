import sqlbuilder
import dbconnection
import main
import joins

StringType = type('')

class SelectResults(object):
    IterationClass = dbconnection.Iteration

    def __init__(self, sourceClass, clause, clauseTables=None,
                 **ops):
        self.sourceClass = sourceClass
        if clause is None or isinstance(clause, str) and clause == 'all':
            clause = sqlbuilder.SQLTrueClause
        if not isinstance(clause, sqlbuilder.SQLExpression):
            clause = sqlbuilder.SQLConstant(clause)
        self.clause = clause
        self.ops = ops
        if self.ops.get('orderBy', sqlbuilder.NoDefault) is sqlbuilder.NoDefault:
            self.ops['orderBy'] = sourceClass.sqlmeta.defaultOrder
        orderBy = self.ops['orderBy']
        if isinstance(orderBy, list) or isinstance(orderBy, tuple):
            orderBy = map(self._mungeOrderBy, orderBy)
        else:
            orderBy = self._mungeOrderBy(orderBy)
        self.ops['dbOrderBy'] = orderBy
        if ops.has_key('connection') and ops['connection'] is None:
            del ops['connection']
            
        tablesDict = sqlbuilder.tablesUsedDict(self.clause, self._getConnection().dbName)
        if clauseTables:
            for table in clauseTables:
                tablesDict[table] = 1
        self.clauseTables = clauseTables
        # Explicitly post-adding-in sqlmeta.table, sqlbuilder.Select will handle sqlrepr'ing and dupes
        self.tables = tablesDict.keys() + [sourceClass.sqlmeta.table]

    def queryForSelect(self):
        columns = [self.sourceClass.q.id] + [getattr(self.sourceClass.q, x.name) for x in self.sourceClass.sqlmeta.columnList]
        query = sqlbuilder.Select(columns,
                                  where=self.clause,
                                  join=self.ops.get('join', sqlbuilder.NoDefault),
                                  distinct=self.ops.get('distinct',False),
                                  lazyColumns=self.ops.get('lazyColumns', False),
                                  start=self.ops.get('start', 0),
                                  end=self.ops.get('end', None),
                                  orderBy=self.ops.get('dbOrderBy',sqlbuilder.NoDefault),
                                  reversed=self.ops.get('reversed', False),
                                  staticTables=self.tables,
                                  forUpdate=self.ops.get('forUpdate', False))
        return query

    def __repr__(self):
        return "<%s at %x>" % (self.__class__.__name__, id(self))

    def __nonzero__(self):
         raise NotImplementedError(
             "To test if a SelectResult will produce any items, use list(result) or result.count()")

    def _getConnection(self):
        return self.ops.get('connection') or self.sourceClass._connection

    def __str__(self):
        conn = self._getConnection()
        return conn.queryForSelect(self)

    def _mungeOrderBy(self, orderBy):
        if isinstance(orderBy, str) and orderBy.startswith('-'):
            orderBy = orderBy[1:]
            desc = True
        else:
            desc = False
        if isinstance(orderBy, (str, unicode)):
            if orderBy in self.sourceClass.sqlmeta.columns:
                val = getattr(self.sourceClass.q, self.sourceClass.sqlmeta.columns[orderBy].name)
                if desc:
                    return sqlbuilder.DESC(val)
                else:
                    return val
            else:
                orderBy = sqlbuilder.SQLConstant(orderBy)
                if desc:
                    return sqlbuilder.DESC(orderBy)
                else:
                    return orderBy
        else:
            return orderBy

    def clone(self, **newOps):
        ops = self.ops.copy()
        ops.update(newOps)
        return self.__class__(self.sourceClass, self.clause,
                              self.clauseTables, **ops)

    def orderBy(self, orderBy):
        return self.clone(orderBy=orderBy)

    def connection(self, conn):
        return self.clone(connection=conn)

    def limit(self, limit):
        return self[:limit]

    def lazyColumns(self, value):
        return self.clone(lazyColumns=value)

    def reversed(self):
        return self.clone(reversed=not self.ops.get('reversed', False))

    def distinct(self):
        return self.clone(distinct=True)

    def newClause(self, new_clause):
        return self.__class__(self.sourceClass, new_clause,
                              self.clauseTables, **self.ops)

    def filter(self, filter_clause):
        if filter_clause is None:
            # None doesn't filter anything, it's just a no-op:
            return self
        clause = self.clause
        if isinstance(clause, (str, unicode)):
            clause = sqlbuilder.SQLConstant('(%s)' % self.clause)
        return self.newClause(sqlbuilder.AND(clause, filter_clause))

    def __getitem__(self, value):
        if type(value) is type(slice(1)):
            assert not value.step, "Slices do not support steps"
            if not value.start and not value.stop:
                # No need to copy, I'm immutable
                return self

            # Negative indexes aren't handled (and everything we
            # don't handle ourselves we just create a list to
            # handle)
            if (value.start and value.start < 0) \
               or (value.stop and value.stop < 0):
                if value.start:
                    if value.stop:
                        return list(self)[value.start:value.stop]
                    return list(self)[value.start:]
                return list(self)[:value.stop]


            if value.start:
                assert value.start >= 0
                start = self.ops.get('start', 0) + value.start
                if value.stop is not None:
                    assert value.stop >= 0
                    if value.stop < value.start:
                        # an empty result:
                        end = start
                    else:
                        end = value.stop + self.ops.get('start', 0)
                        if self.ops.get('end', None) is not None \
                           and value['end'] < end:
                            # truncated by previous slice:
                            end = self.ops['end']
                else:
                    end = self.ops.get('end', None)
            else:
                start = self.ops.get('start', 0)
                end = value.stop + start
                if self.ops.get('end', None) is not None \
                   and self.ops['end'] < end:
                    end = self.ops['end']
            return self.clone(start=start, end=end)
        else:
            if value < 0:
                return list(iter(self))[value]
            else:
                start = self.ops.get('start', 0) + value
                return list(self.clone(start=start, end=start+1))[0]

    def __iter__(self):
        # @@: This could be optimized, using a simpler algorithm
        # since we don't have to worry about garbage collection,
        # etc., like we do with .lazyIter()
        return iter(list(self.lazyIter()))

    def lazyIter(self):
        """
        Returns an iterator that will lazily pull rows out of the
        database and return SQLObject instances
        """
        conn = self._getConnection()
        return conn.iterSelect(self)

    def accumulate(self, *expressions):
        """ Use accumulate expression(s) to select result
            using another SQL select through current
            connection.
            Return the accumulate result
        """
        conn = self._getConnection()
        exprs = []
        for expr in expressions:
            if not isinstance(expr, sqlbuilder.SQLExpression):
                expr = sqlbuilder.SQLConstant(expr)
            exprs.append(expr)
        return conn.accumulateSelect(self, *exprs)

    def count(self):
        """ Counting elements of current select results """
        assert not (self.ops.get('distinct') and (self.ops.get('start')
                                                  or self.ops.get('end'))), \
               "distinct-counting of sliced objects is not supported"
        if self.ops.get('distinct'):
            # Column must be specified, so we are using unique ID column.
            # COUNT(DISTINCT column) is supported by MySQL and PostgreSQL,
            # but not by SQLite. Perhaps more portable would be subquery:
            #  SELECT COUNT(*) FROM (SELECT DISTINCT id FROM table)
            count = self.accumulate('COUNT(DISTINCT %s.%s)' % (
                                             self.sourceClass.sqlmeta.table,
                                             self.sourceClass.sqlmeta.idName))
        else:
            count = self.accumulate('COUNT(*)')
        if self.ops.get('start'):
            count -= self.ops['start']
        if self.ops.get('end'):
            count = min(self.ops['end'] - self.ops.get('start', 0), count)
        return count

    def accumulateMany(self, *attributes):
        """ Making the expressions for count/sum/min/max/avg
            of a given select result attributes.
            `attributes` must be a list/tuple of pairs (func_name, attribute);
            `attribute` can be a column name (like 'a_column')
            or a dot-q attribute (like Table.q.aColumn)
        """
        expressions = []
        for func_name, attribute in attributes:
            if type(attribute) == StringType:
                expression = '%s(%s)' % (func_name, attribute)
            else:
                expression = getattr(sqlbuilder.func, func_name)(attribute)
            expressions.append(expression)
        return self.accumulate(*expressions)

    def accumulateOne(self, func_name, attribute):
        """ Making the sum/min/max/avg of a given select result attribute.
            `attribute` can be a column name (like 'a_column')
            or a dot-q attribute (like Table.q.aColumn)
        """
        return self.accumulateMany((func_name, attribute))

    def sum(self, attribute):
        return self.accumulateOne("SUM", attribute)

    def min(self, attribute):
        return self.accumulateOne("MIN", attribute)

    def avg(self, attribute):
        return self.accumulateOne("AVG", attribute)

    def max(self, attribute):
        return self.accumulateOne("MAX", attribute)

    def getOne(self, default=sqlbuilder.NoDefault):
        """
        If a query is expected to only return a single value,
        using ``.getOne()`` will return just that value.

        If not results are found, ``SQLObjectNotFound`` will be
        raised, unless you pass in a default value (like
        ``.getOne(None)``).

        If more than one result is returned,
        ``SQLObjectIntegrityError`` will be raised.
        """
        results = list(self)
        if not results:
            if default is sqlbuilder.NoDefault:
                raise main.SQLObjectNotFound(
                    "No results matched the query for %s"
                    % self.sourceClass.__name__)
            return default
        if len(results) > 1:
            raise main.SQLObjectIntegrityError(
                "More than one result returned from query: %s"
                % results)
        return results[0]

    def throughTo(self):
        return _throughTo_getter(self)
    throughTo = property(throughTo)

    def _throughTo(self, attr):
        ref = self.sourceClass.sqlmeta.columns.get(attr.endswith('ID') and attr or attr+'ID', None)
        if ref and ref.foreignKey:
            return self._throughToFK(ref)
        else:
            join = [x for x in self.sourceClass.sqlmeta.joins if x.joinMethodName==attr]
            if join:
                join = join[0]
                if hasattr(join, 'otherColumn'):
                    return self._throughToRelatedJoin(join)
                return self._throughToMultipleJoin(join)
        
        raise AttributeError("throughTo argument (got %s) should be name of foreignKey or SQL*Join in %s" % (attr, self.sourceClass))
    
    def _throughToFK(self, col):
        otherClass = getattr(self.sourceClass, "_SO_class_"+col.foreignKey)
        query = sqlbuilder.Alias(self.queryForSelect(), "%s_%s" % (self.sourceClass.__name__, col.name))
        return otherClass.select(otherClass.q.id==getattr(query.q, getattr(self.sourceClass.q, col.name).fieldName),
                                distinct=True)
        
    def _throughToMultipleJoin(self, join):
        otherClass = join.otherClass
        query = self.queryForSelect()
        query = sqlbuilder.Alias(query, "%s_%s" % (self.sourceClass.__name__, join.joinMethodName))
        joinColumn = getattr(otherClass.q, join.soClass.sqlmeta.style.dbColumnToPythonAttr(join.joinColumn))
        return otherClass.select(joinColumn == getattr(query.q, self.sourceClass.q.id.fieldName),
                                distinct=True,
                                orderBy=join.orderBy)
    
    def _throughToRelatedJoin(self, join):
        otherClass = join.otherClass
        intTable = sqlbuilder.Table(join.intermediateTable)
        print join.joinColumn, join.otherColumn
        query = self.queryForSelect().newItems([getattr(intTable, join.joinColumn)])
        query = sqlbuilder.Alias(query, "%s_%s" % (self.sourceClass.__name__, join.joinMethodName))
        print query
        clause = sqlbuilder.AND(otherClass.q.id == getattr(intTable, join.otherColumn),
                     getattr(intTable, join.joinColumn) == getattr(query.q, join.joinColumn))
        ret = otherClass.select(clause,
                                distinct=True,
                                orderBy=join.orderBy)
        print ret
        return ret


class _throughTo_getter(object):
    def __init__(self, inst):
        self.sresult = inst
    def __getattr__(self, attr):
        return self.sresult._throughTo(attr)

__all__ = ['SelectResults']
