from py.test import raises
from sqlobject import *
from sqlobject.tests.dbtest import *
from sqlobject.materialized import *

dep = DependencyManager()

class DependencyOne(SQLObject):
    name = StringCol()
    twos = SQLMultipleJoin('DependencyTwo', joinColumn='one_id')
    
    @dep.dependentOn('DependencyOne', 'twos')
    def _get_twoCount(self):
        return self.twos.count()
    
    @dep.dependentOn('DependencyTwo', 'length')
    def _get_detailSize(self):
        return self.twos.sum('length')

class DependencyTwo(SQLObject):
    detail = StringCol()
    length = IntCol()
    one = ForeignKey('DependencyOne')
    
    @dep.dependentOn('DependencyOne', 'name')
    def _get_name(self):
        return self.one.name
    
    @dep.dependentOn('DependencyTwo', 'detail')
    @dep.dependentOn('DependencyOne', 'name')
    def _get_name2(self):
        return self.one.name + self.detail
    
def setup_module(mod):
    setupClass([DependencyOne, DependencyTwo])
    
def testSetup():
    assert dep.get('DependencyOne', 'name') == set([('DependencyTwo','_get_name'),
                                                        ('DependencyTwo','_get_name2')])