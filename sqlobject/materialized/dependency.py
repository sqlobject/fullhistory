from sqlobject import declarative
from sqlobject import sqlbuilder
from sqlobject import classregistry
from sqlobject import SQLObject

import inspect

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
        if not isinstance(source[0], (list, tuple)):
            source = (source,)
        def decorate(func):
            depOn = getattr(func, '_dependentOn', [])
            for s in source:
                depOn.append((self, factory, s))
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
        if target[1][:5] == '_get_':
            target = (target[0], target[1][5:])
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
            
            if inspect.isfunction(route):
                argspec = inspect.getargspec(route)
                if len(argspec[0]) == len(argspec[3]) and not argspec[1] and not argspec[2]:
                    route = route()
            if route is None:
                route = lambda i,c: [i]
            elif isinstance(route, sqlbuilder.SQLExpression):
                route = lambda i,c,r=route: c.select(sqlbuilder.AND(r, i), connection=i._connection)
            
            ret.setdefault((targetClassName, targetAttr),[]).extend(route(inst, targetClass))
        return ret
        
    def process(self, inst, attrs, _toProcess=None):
        if _toProcess is None:
            _toProcess = self.instancesToProcess(inst, attrs)
        for (cls, attr), insts in _toProcess.iteritems():
            for inst in insts:
                changed = inst._SO_updateCacheObject([attr])
                if changed:
                    self.process(inst, [attr])


dep = SQLDependencyManager()

dependentOn = dep.dependentOn
