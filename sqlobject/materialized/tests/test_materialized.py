from py.test import raises
from sqlobject import *
from sqlobject.tests.dbtest import *
from sqlobject.materialized import *
from sqlobject.sqlbuilder import ImportProxy

MO = ImportProxy('MaterializedOne')
MT = ImportProxy('MaterializedTwo')


class MaterializedOne(MaterializedSQLObject):
    name = StringCol()
    twos = SQLMultipleJoin('MaterializedTwo', joinColumn='oneID')
    
    @cachedAs(IntCol)
    @dep.dependentOn('MaterializedOne', 'twos')
    def _get_twoCount(self):
        print "getting twoCount", self.twos.count()
        return self.twos.count()
    
    @cachedAs(IntCol)
    @dep.dependentOn('MaterializedTwo', 'length', MO.q.twos)
    def _get_detailSize(self):
        return self.twos.sum('length')

class MaterializedTwo(MaterializedSQLObject):
    class sqlmeta:
        cachedIn = 'cache_materialized_two'
    
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
    
    @cachedAs(StringCol)
    @dep.dependentOn('MaterializedTwo', 'detail')
    @dep.dependentOn('MaterializedTwo', 'name')
    def _get_name3(self):
        ''' dep on dep '''
        return self.name + self.detail
    
def setup_module(mod):
    setupClass([MaterializedOne, MaterializedTwo, MaterializedOne.sqlmeta.cacheClass, MaterializedTwo.sqlmeta.cacheClass])
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
    assert set([(x,y) for x,y,z in deps]) == set([('MaterializedTwo','name'),
                                               ('MaterializedTwo','name2')])
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
                                                         ('MaterializedTwo', 'name'): [twos[0], twos[1]],
                                                         ('MaterializedTwo', 'name2'): [twos[0], twos[1]],
                                                         }
    
def testCacheClass():
    assert MaterializedOne.sqlmeta.cacheClass.sqlmeta.table == 'materialized_one_cache'
    # Overridden by MaterializedTow.sqlmeta.cachedIn
    assert MaterializedTwo.sqlmeta.cacheClass.sqlmeta.table == 'cache_materialized_two'
    
    assert 'name' in MaterializedTwo.sqlmeta.cacheClass.sqlmeta.columns

def testManualCacheObject():
    obj = twos[0]
    assert hasattr(obj, '_SO_cacheObject')
    assert obj._SO_cacheObject.name == None
    obj._SO_cacheObject.name = 'Not Right'
    obj._SO_cacheObject.name_dirty = False
    assert obj._SO_cacheObject.name == 'Not Right'
    
def testDepSettingCacheObject():
    obj = twos[1]
    assert obj._SO_cacheObject.name == None
    dep.process(obj.one, ['name'])
    assert obj._SO_cacheObject.name == obj._get_name.func(obj)

def testModificationSettingCacheObject():
    obj = twos[2]
    assert obj._SO_cacheObject.name == None
    obj.one.name = 'G'
    assert obj._SO_cacheObject.name == 'G'
    assert obj.name == 'G'

def testJoinUpdateSettingCacheObject():
    obj = ones[0]
    prev = obj.detailSize
    obj.twos[0].length += 4
    assert obj._SO_cacheObject.detailSize == prev + 4
    assert obj.detailSize == prev + 4

def testJoinAdditionSettingCacheObject():
    obj = ones[1]
    prevS = obj.detailSize
    prevC = obj.twoCount
    new_two = MaterializedTwo(detail='f', length=4, one=obj)
    assert obj._SO_cacheObject.detailSize == prevS + new_two.length
    assert obj._SO_cacheObject.twoCount == prevC + 1
    assert obj.detailSize == prevS + new_two.length
    
def testJoinDeletionSettingCacheObject():
    obj = ones[1]
    prevS = obj.detailSize
    prevC = obj.twoCount
    new_two = MaterializedTwo(detail='g', length=5, one=obj)
    assert obj._SO_cacheObject.detailSize == prevS + new_two.length
    assert obj._SO_cacheObject.twoCount == prevC + 1
    new_two.destroySelf()
    assert obj.twos.count() == prevC
    assert obj._SO_cacheObject.detailSize == prevS
    assert obj._SO_cacheObject.twoCount == prevC