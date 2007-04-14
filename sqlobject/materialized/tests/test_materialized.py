from py.test import raises
from sqlobject import *
from sqlobject.tests.dbtest import *
from sqlobject.materialized import *

class MaterializedOne(MaterializedSQLObject):
    name = StringCol()
    twos = SQLMultipleJoin('MaterializedTwo', joinColumn='one_id')
    
    @cachedAs(IntCol)
    @dep.dependentOn(('MaterializedOne', 'twos'))
    def _get_twoCount(self):
        return self.twos.count()
    
    @cachedAs(IntCol)
    @dep.dependentOn(('MaterializedTwo', 'length'))
    def _get_detailSize(self):
        return self.twos.sum('length')

class MaterializedTwo(MaterializedSQLObject):
    detail = StringCol()
    length = IntCol()
    one = ForeignKey('MaterializedOne')
    
    @cachedAs(StringCol)
    @dep.dependentOn(('MaterializedOne', 'name'))
    def _get_name(self):
        return self.one.name
    
    @cachedAs(StringCol)
    @dep.dependentOn(('MaterializedTwo', 'detail'))
    @dep.dependentOn(('MaterializedOne', 'name'))
    def _get_name2(self):
        return self.one.name + self.detail
    
def setup_module(mod):
    setupClass([MaterializedOne, MaterializedTwo])
    
def testSetup():
    assert dep.get(('MaterializedOne', 'name')) == set([('MaterializedTwo','_get_name'),
                                                        ('MaterializedTwo','_get_name2')])
    
def testSetupDepsForFK():
    assert dep.get(('MaterializedOne', 'id')) == set([('MaterializedTwo', 'oneID')])
    
def testSetupDepsForJoin():
    assert dep.get(('MaterializedTwo', 'id')) == set([('MaterializedOne', 'twos')])
    
    