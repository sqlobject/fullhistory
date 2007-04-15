from py.test import raises
from sqlobject import *
from sqlobject.tests.dbtest import *
from sqlobject.materialized import *
from sqlobject.sqlbuilder import ImportProxy

MO = ImportProxy('MaterializedOne')
MT = ImportProxy('MaterializedTwo')


class MaterializedOne(MaterializedSQLObject):
    name = StringCol()
    twos = SQLMultipleJoin('MaterializedTwo', joinColumn='one_id')
    
    @cachedAs(IntCol)
    @dep.dependentOn('MaterializedOne', 'twos', MO.q.twos)
    def _get_twoCount(self):
        return self.twos.count()
    
    @cachedAs(IntCol)
    @dep.dependentOn('MaterializedTwo', 'length', MO.q.twos)
    def _get_detailSize(self):
        return self.twos.sum('length')

class MaterializedTwo(MaterializedSQLObject):
    detail = StringCol()
    length = IntCol()
    one = ForeignKey('MaterializedOne')
    
    @cachedAs(StringCol)
    @dep.dependentOn('MaterializedOne', 'name', MT.q.one)
    def _get_name(self):
        return self.one.name
    
    @cachedAs(StringCol)
    @dep.dependentOn('MaterializedTwo', 'detail')
    @dep.dependentOn('MaterializedOne', 'name', MT.q.one)
    def _get_name2(self):
        return self.one.name + self.detail
    
def setup_module(mod):
    setupClass([MaterializedOne, MaterializedTwo])
    mod.ones = inserts(MaterializedOne, (('S',),
                                         ('T',),
                                        ), 'name')
    mod.twos = inserts(MaterializedTwo, (('a',   2,     mod.ones[0].id),
                                         ('b',   3,     mod.ones[0].id),
                                         ('c',   4,     mod.ones[1].id),
                                         ('d',   5,     mod.ones[1].id),
                                        ), 'detail length oneID')
    
def testSetup():
    deps = dep.get('MaterializedOne', 'name')
    assert set([(x,y) for x,y,z in deps]) == set([('MaterializedTwo','_get_name'),
                                               ('MaterializedTwo','_get_name2')])
    assert [z for x,y,z in deps][0] != [None, None]
    
def testSetupDepsForFK():
    # Can't test route directly
    deps = dep.get('MaterializedOne', 'id')
    assert [(x,y) for x,y,z in deps][0] == ('MaterializedTwo', 'oneID')
    assert [z for x,y,z in deps][0] is not None
    
def testSetupDepsForJoin():
    deps = dep.get('MaterializedTwo', 'id')
    assert set((x,y) for x,y,z in deps) == set([('MaterializedOne', 'twos')])
    assert [z for x,y,z in deps][0] is not None
    
    
def testProcessInstances():
    
    assert dep.instancesToProcess(ones[0], ['name']) == {
                                                         ('MaterializedTwo', '_get_name'): [twos[0], twos[1]],
                                                         ('MaterializedTwo', '_get_name2'): [twos[0], twos[1]],
                                                         }