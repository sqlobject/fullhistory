from py.test import raises
from sqlobject import *
from sqlobject.tests.dbtest import *
from sqlobject.materialized import *
from sqlobject.sqlbuilder import ImportProxy, AND

MO = ImportProxy('MaterializedOne')
MT = ImportProxy('MaterializedTwo')


class MaterializedOne(MaterializedSQLObject):
    name = StringCol()
    twos = SQLMultipleJoin('MaterializedTwo', joinColumn='oneID')
    
    @cachedAs(IntCol)
    @dependentOn('MaterializedOne', 'twos')
    def _get_twoCount(self):
        print "getting twoCount", self.twos.count()
        return self.twos.count()
    
    @cachedAs(IntCol)
    @dependentOn('MaterializedTwo', 'length', MO.q.twos)
    def _get_detailSize(self):
        return self.twos.sum('length')

    @cachedAs(IntCol)
    @dependentOn('MaterializedTwo', 'id', AND(MO.q.twos, MT.q.length<=4))
    def _get_smallTwoCount(self):
        return self.twos.filter(MT.q.length<=4).count()
    
    @dependentOn('MaterializedTwo', 'id', AND(MO.q.twos, MT.q.length<=4))
    def _get_smallTwos(self):
        print "getting small twos"
        return self.twos.filter(MT.q.length<=4)
    
    @cachedAs(IntCol)
    @dependentOn('MaterializedOne', 'smallTwos')
    def _get_smallTwoCount2(self):
        return self.smallTwos.count()

class MaterializedTwo(MaterializedSQLObject):
    class sqlmeta:
        cachedIn = 'cache_materialized_two'
    
    detail = StringCol()
    length = IntCol()
    one = ForeignKey('MaterializedOne')
    
    @cachedAs(StringCol)
    @dependentOn('MaterializedOne', 'name', MT.q.one)
    def _get_name(self):
        return self.one.name
    
    @cachedAs(StringCol)
    @dependentOn('MaterializedTwo', 'detail')
    @dependentOn('MaterializedOne', 'name', MT.q.one)
    def _get_name2(self):
        return self.one.name + self.detail
    
    @cachedAs(StringCol)
    @dependentOn('MaterializedTwo', 'detail')
    @dependentOn('MaterializedTwo', 'name')
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
    assert ('MaterializedOne', 'twos') in set((x,y) for x,y,z in deps)
    assert None not in [z for x,y,z in deps]
    
    
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

def testChainedModificationSettingCacheObject():
    obj = twos[2]
    obj.one.name = 'G'
    assert obj._SO_cacheObject.name3 == 'G'+obj.detail
    assert obj.name3 == 'G'+obj.detail

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
    assert obj._SO_cacheObject.detailSize == prevS
    assert obj._SO_cacheObject.twoCount == prevC

def testRestrictedDependencyRoute():
    assert twos[1].length <= 4
    insts = dep.instancesToProcess(twos[1], ['id'])
    assert insts[('MaterializedOne', 'smallTwoCount')] == [ones[0]]
    assert twos[3].length > 4
    insts = dep.instancesToProcess(twos[3], ['id'])
    assert insts[('MaterializedOne', 'smallTwoCount')] == []
    
def testRestrictedDependencyRoute2():
    assert twos[1].length <= 4
    insts = dep.instancesToProcess(twos[1], ['id'])
    assert insts[('MaterializedOne', 'smallTwos')] == [ones[0]]
    assert twos[3].length > 4
    insts = dep.instancesToProcess(twos[3], ['id'])
    assert insts[('MaterializedOne', 'smallTwos')] == []