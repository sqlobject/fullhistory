from sqlobject import *
from sqlobject.tests.dbtest import *
from sqlobject.sqlbuilder import ImportProxy

def testSimple():
    nyi = ImportProxy('NotYetImported')
    x = nyi.q.name
    
    class NotYetImported(SQLObject):
        name = StringCol(dbName='a_name')
    
    assert str(x) == 'not_yet_imported.a_name'