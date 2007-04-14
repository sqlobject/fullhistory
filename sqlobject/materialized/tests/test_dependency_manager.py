from py.test import raises
from sqlobject import *
from sqlobject import declarative
from sqlobject.tests.dbtest import *
from sqlobject.materialized import *

from pprint import pprint

class A(object):
    name = 'A'
    
class B(object):
    c = True
    a = A()
    
    def name(self):
        return self.a.name
    
    def condName(self):
        if self.c:
            return self.a.name
        else:
            return 'B'

def test_addDependency():
    dep = DependencyManager()
    dep.add(('B', 'name'), ('A', 'name'))
    assert dep.get(('A', 'name')) == set([('B', 'name')])

def test_addDependencies():
    dep = DependencyManager()
    dep.add(('B', 'name'),     ('A', 'name'))
    dep.add(('B', 'condName'), ('A', 'name'))
    assert dep.get(('A', 'name')) == set((('B', 'name'),
                                          ('B', 'condName')))

def test_addDepWithDecorator():
    dep = DependencyManager()
    
    class C(declarative.Declarative):
        
        a = A()        
        
        @dep.dependentOn(('A', 'name'))
        def name(self):
            return self.a.name

    assert dep.get(('A', 'name')) == set([('C', 'name')])

def test_addDepsWithDecorator():
    dep = DependencyManager()
    
    class C(declarative.Declarative):
        
        a = A()
        b = B()
        
        @dep.dependentOn(('B', 'name'))
        @dep.dependentOn(('A', 'name'))
        def name(self):
            return self.a.name + self.b.name

    assert dep.get(('A', 'name')) == set([('C', 'name')])
    assert dep.get(('B', 'name')) == set([('C', 'name')])
    
    

def test_depTreeFrom1():
    dep = DependencyManager()
    dep.add(('B', 'name'), ('A', 'name'))
    assert dep.treeFrom(('A', 'name')) == {
                                          ('B','name'): (('B','name'), {})
                                          }

def test_depTreeFrom2():
    dep = DependencyManager()
    dep.add(('B', 'name'),  ('A', 'name'))
    dep.add(('D', 'name'),  ('A', 'name'))
    dep.add(('C', 'cname'), ('B', 'name'))
    pprint(dep.treeFrom(('A', 'name')))
    assert dep.treeFrom(('A', 'name')) == {
                                          ('B','name'): (('B', 'name'),
                                                         {
                                                         ('C', 'cname'): (('C', 'cname'),{})
                                                         }),
                                          ('D','name'): (('D','name'), {})
                                          }

def test_depTreeTo1():
    dep = DependencyManager()
    dep.add(('B', 'name'),  ('A', 'name'))
    dep.add(('D', 'name'),  ('A', 'name'))
    dep.add(('C', 'cname'), ('B', 'name'))
    pprint(dep.treeTo(('C', 'cname')))
    assert dep.treeTo(('C', 'cname')) == {
                                         ('B','name'): (('B','name'),
                                                        {
                                                        ('A', 'name'): (('A', 'name'),{})
                                                        }),
                                         }

def test_customItems():
    class DItem(DependencyItem):
        def __init__(self, (a,b,c)):
            self.key = (a,b)
            self.value = (b,c)

    class Dep(Dependency):
        DependencyItemClass = DItem
    
    dep = DependencyManager()
    dep.DependencyClass = Dep
    
    dep.add(('b', 'x', 1), ('a', 'w', 1))
    dep.add(('c', 'x', 2), ('a', 'w', 1))
    dep.add(('d', 'y', 2), ('b', 'x', 1))
    assert dep.treeFrom(('a','w')) == {
                                ('b','x'): (('x', 1),
                                            {
                                            ('d', 'y'): (('y', 2), {})
                                            }),
                                ('c','x'): (('x', 2), {})
                                }
    
    assert dep.get(('a','w')) == set([('x', 1), ('x', 2)])
    

#def test_addDepWithSignal():
#    pass
