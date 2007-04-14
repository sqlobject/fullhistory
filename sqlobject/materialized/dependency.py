from sqlobject import declarative
from sqlobject.events import *
from sqlobject.include.pydispatch import dispatcher

from operator import isCallable

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
    def __init__(self, item):
        self.key = item
        self.value = item

class Dependency(object):
    DependencyItemClass = DependencyItem
    DecoratorFactory = depFromClsAttr
    
    def __init__(self, target, source, **extra):
        self.target = self.DependencyItemClass(target)
        self.source = self.DependencyItemClass(source)

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
    
    def get(self, source):
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
    
    
    def dependentOn(self, source, factory=None):
        ''' decorator '''
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
        elif len(args) == 2:
            dep = self.DependencyClass(*args)
        else:
            raise TypeError('Incorrect arguments to add(), expected %s instance or (target,source), got: %s' % (self.DependencyClass.__name__, args))
        return dep

#if isinstance(inst, str):
#    sourceClass = inst
#else:
#    sourceClass = inst.__class__.__name__

class SQLDependency(Dependency):
    pass

class SQLDependencyManager(DependencyManager):
    
    def process(self, inst, attrs):
        ''' find and recalculate items dependent on attrs'''
        sourceClass = inst.__class__.__name__
        deps = self.getMany((sourceClass, attr) for attr in attrs)
        for targetClass, targetAttr, route in deps:
            targetClass = getClassFromName(targetClass)
            for t_inst in route(inst, targetClass):
                t_inst._SO_resetCache(targetAttr)
        

    
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
