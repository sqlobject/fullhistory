from sqlobject import *
from sqlobject.sqlbuilder import *
from sqlobject.tests.dbtest import *

''' Tests retrieving objects through a join/fk on a selectResults
'''

class SRThrough1(SQLObject):
    three = ForeignKey('SRThrough3')
    twos  = SQLMultipleJoin('SRThrough2', joinColumn='oneID')

class SRThrough2(SQLObject):
    one = ForeignKey('SRThrough1')
    threes = SQLRelatedJoin('SRThrough3', addRemoveName='Three')

class SRThrough3(SQLObject):
    name = StringCol()
    ones = SQLMultipleJoin('SRThrough1', joinColumn='threeID')
    twos = SQLRelatedJoin('SRThrough2')


def setup_module(mod):
    setupClass([mod.SRThrough3, mod.SRThrough1, mod.SRThrough2])
    threes = inserts(mod.SRThrough3,
                     [('a',),('b',),('c',)],
                     'name')
    ones = inserts(mod.SRThrough1,
                     [(threes[0].id,),(threes[0].id,),(threes[2].id,)],
                     'threeID')
    twos = inserts(mod.SRThrough2,
                     [(ones[0].id,),(ones[1].id,),(ones[2].id,)],
                     'oneID')
    twos[0].addThree(threes[0])
    twos[0].addThree(threes[1])
    mod.threes = threes
    mod.twos   = twos
    mod.ones   = ones

def testThroughFK():
    assert list(threes[0].ones.throughTo.three) == [threes[0]]

def testThroughMultipleJoin():
    assert list(threes[0].ones.throughTo.twos) == [twos[0], twos[1]]
    
def testThroughRelatedJoin():
    print [list(x.threes) for x in threes[0].twos]
    assert list(threes[0].twos.throughTo.threes) == [threes[0], threes[1]]

    