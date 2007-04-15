from sqlobject import declarative
from sqlobject import sqlbuilder
from sqlobject.events import *
from sqlobject.include.pydispatch import dispatcher
from sqlobject import classregistry

from operator import isCallable
import inspect

def _processDependentOn(new_class_name, bases, new_attrs, post_funcs, early_funcs):
    def f(cls):
        for name, value in new_attrs.items():
            if hasattr(value, '_dependentOn'):
                for dep, maker, source in value._dependentOn:
                    dep.add(maker(cls, name, value),source)
    early_funcs.append(f)

listen(_processDependentOn, dispatcher.Any, ClassCreateSignal)

class depFromClsAttr(object):
    def __call__(self, cls, name, value):
        return (cls.__name__, name)


class DependencyItem(object):
    def __init__(self, item, *extras):
        self.key = item
        self.value = item + extras

class Dependency(object):
    DependencyItemClass = DependencyItem
    DecoratorFactory = depFromClsAttr
    
    def __init__(self, target, source, *extras):
        self.target = self.DependencyItemClass(target, *extras)
        self.source = self.DependencyItemClass(source, *extras)

class DependencyManager(object):
    DependencyClass = Dependency
    
    def __init__(self):
        self._deps = {}
        self._reverseDeps = {}
    
    def add(self, *args):
        ''' Add a dependency. '''
        dep = self.dependencyForAdd(*args)
        self._deps.setdefault(dep.source.key, set()).add(dep.target)
        self._reverseDeps.setdefault(dep.target.key, set()).add(dep.source)
    
    def get(self, *source):
        ''' get immediate dependent values for source '''
        return self.getMany([source])
    
    def getMany(self, sources):
        deps = (self.treeFrom(source, depth=1) for source in sources)
        return reduce(set.union, (set(y[0] for y in x.values()) for x in deps), set())
    
    def treeFrom(self, source, depth=None):
        ''' dictionary of dependencies with their dependenices arising from source'''
        return self._tree(self._deps, source, depth=depth)
    
    def treeTo(self, source, depth=None):
        ''' dictionary of dependencies with their dependenices leading to source'''
        return self._tree(self._reverseDeps, source, depth=depth)
    
    def _tree(self, deps, source, depth=None):
        if depth == 0:
            return {}
        if depth:
            depth = depth-1

        return dict(((target.key, (target.value, self._tree(deps, target.key, depth=depth)))
                     for target in deps.get(source,set())))
    
    
    def dependentOn(self, *source, **kw):
        ''' decorator '''
        factory = kw.pop('factory', None)
        if factory is None:
            factory = self.DependencyClass.DecoratorFactory()
        def decorate(func):
            depOn = getattr(func, '_dependentOn', [])
            depOn.append((self, factory, source))
            setattr(func, '_dependentOn', depOn)
            return func
        return decorate


    def dependencyForAdd(self, *args):
        if len(args) == 1 and isinstance(args[0], self.DependencyClass):
            dep = args[0]
        else:
            dep = self.DependencyClass(*args)
        return dep

#if isinstance(inst, str):
#    sourceClass = inst
#else:
#    sourceClass = inst.__class__.__name__


class SQLDependency(Dependency):
    def __init__(self, target, source, *extras):
        if isinstance(source[0], sqlbuilder.SQLExpression):
            source = (str(source[0].tableName), str(source[0].fieldName)) + source[1:]
            print source
        if len(source) == 3:
            other = source[2]
            try:
                hash(other)
            except TypeError:
                other = lambda o=other: o
            extras = (other,) + extras
            source = source[:2]
        else:
            extras = (None,) + extras
        self.target = self.DependencyItemClass(target, *extras)
        self.source = self.DependencyItemClass(source, *extras)

class SQLDependencyManager(DependencyManager):
    DependencyClass = SQLDependency
    
    def instancesToProcess(self, inst, attrs):
        ''' find items dependent on attrs'''
        ret = {}
        sourceClass = inst.__class__.__name__
        deps = self.getMany((sourceClass, attr) for attr in attrs)
        for targetClassName, targetAttr, route in deps:
            
            targetClass = classregistry.findClass(targetClassName, class_registry=inst.sqlmeta.registry)
            
            print "Route", route
            if inspect.isfunction(route):
                argspec = inspect.getargspec(route)
                print "argspec", argspec
                if len(argspec[0]) == len(argspec[3]) and not argspec[1] and not argspec[2]:
                    print "exec", route()
                    route = route()
            print "RouteX", route
            if route is None:
                route = lambda i,c: [i]
            elif isinstance(route, sqlbuilder.SQLExpression):
                route = lambda i,c,r=route: c.select(sqlbuilder.AND(r, i))
            
            ret.setdefault((targetClassName, targetAttr),[]).extend(route(inst, targetClass))
        return ret
        
    def process(self, inst, attrs):
        for (cls, attr), insts in self.instancesToProcess(inst, attrs).iteritems():
            for inst in insts:
                inst._SO_resetCache(attr)

    
    def FK(self, name):
        '''route for foreign key'''
        def f(inst, cls):
            return cls.select(cls.q.id==getattr(inst, name+'ID'))
        return f
    
    def J(self, name):
        '''route for join'''
        def f(inst, cls):
            return cls.select(getattr(cls.q,name)==inst.id)
        return f
    
    def S(self, *args):
        '''route for self'''
        if not len(args):
            return self.S
        return args[0]

dep = SQLDependencyManager()
