from sqlobject import *
from sqlobject.tests.dbtest import *

########################################
## Indexes
########################################

class SOIndex1(SQLObject):
    name = StringCol(length=100)
    anumber = IntCol()

    #nameIndex = DatabaseIndex('name', unique=True)
    nameIndex2 = DatabaseIndex(name, anumber)
    #nameIndex3 = DatabaseIndex({'column': name,
    #                            'length': 3})
    nameIndex3 = DatabaseIndex({'column': name,
                                'length': 3},  unique=True)

class SOIndex2(SQLObject):

    name = StringCol()

    nameIndex = DatabaseIndex({'expression': 'lower(name)'})

def test_1():
    setupClass(SOIndex1)
    n = 0
    for name in 'blah blech boring yep yort snort'.split():
        n += 1
        SOIndex1(name=name, anumber=n)
    mod = SOIndex1._connection.module
    try:
        SOIndex1(name='blah', anumber=0)
    except (mod.ProgrammingError, mod.IntegrityError, mod.OperationalError, mod.DatabaseError):
        # expected
        pass
    else:
        assert 0, "Exception expected."

def test_2():
    if not supports('expressionIndex'):
        return
    setupClass(SOIndex2)
    SOIndex2(name='')
