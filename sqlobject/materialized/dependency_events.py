from sqlobject import events
from sqlobject import SQLObject
from dependency import dep

def _processDependentOn(new_class_name, bases, new_attrs, post_funcs, early_funcs):
    def f(cls):
        for name, value in new_attrs.items():
            if hasattr(value, '_dependentOn'):
                for dep, maker, source in value._dependentOn:
                    dep.add(maker(cls, name, value),source)
    early_funcs.append(f)

events.listen(_processDependentOn, events.dispatcher.Any, events.ClassCreateSignal)

def _processDepsOnChange(inst, kwargs):
    dep.process(inst, kwargs.keys())

events.listen(_processDepsOnChange, SQLObject, events.RowUpdatedSignal)
events.listen(_processDepsOnChange, SQLObject, events.RowCreatedSignal)

def _processDepsOnDelete(inst, post_funcs):
    attrs = ['id'] + inst.sqlmeta.columns.keys()
    toProcess = dep.instancesToProcess(inst, attrs)
    def f(inst, toProcess=toProcess):
        dep.process(inst, attrs, _toProcess=toProcess)
    if toProcess:
        post_funcs.append(f)
events.listen(_processDepsOnDelete, SQLObject, events.RowDestroySignal)