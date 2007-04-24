from sqlobject import *
from sqlobject.sqlbuilder import ImportProxy
from sqlobject.tests.dbtest import *

class DerivePerson(SQLObject):
    name = StringCol(alternateID=True)
    city = StringCol()
    phones = SQLMultipleJoin('DerivePhone', joinColumn='personID')
    
    primaryPhone = OneFrom('phones', query=ImportProxy('DerivePhone').q.isPrimary==True)
    secondaryPhones = FilterFrom('phones', query=ImportProxy('DerivePhone').q.isPrimary==False)
    
    primaryMinutes = ValueFrom('primaryPhone', ImportProxy('DerivePhone').q.totalMinutes)
    mostMinutes = MaxFrom('phones', ImportProxy('DerivePhone').q.totalMinutes)
    mostSecondaryMinutes = MaxFrom('secondaryPhones', ImportProxy('DerivePhone').q.totalMinutes)
#    mostUsedPhone = OneFrom('phones', query=ImportProxy('DerivePhone').q.totalMinutes==SelfProxy.q.mostMinutes)
    

class DerivePhone(SQLObject):
    number = StringCol(alternateID=True)
    isPrimary = BoolCol()
    
    city = ValueFrom('person', ImportProxy('Person').q.city)
    totalMinutes = SumFrom('calls', ImportProxy('DerivePhoneCall').q.minutes)
#    totalLocalMinutes = SumFrom('calls', ImportProxy('DerivedPhoneCall').q.minutes,
#            query=ImportProxy('DerivedPhoneCall').q.city == SelfProxy.q.city)
    
    person = ForeignKey('DerivePerson')
    calls = SQLMultipleJoin('DerivePhoneCall', joinColumn='phoneID')
    
class DerivePhoneCall(SQLObject):
    #onDate = DateTimeCol(default="now")
    minutes = IntCol()
    city = StringCol()
    
    phone = SQLForeignKey('DerivePhone')


def setup_module(mod):
    setupClass([DerivePerson, DerivePhone, DerivePhoneCall], force=True)
    ppl = inserts(DerivePerson, [{'name':'Luke', 'city':'Chicago'}])
    phones = inserts(DerivePhone, [
            {'number':'1234', 'person':ppl[0], 'isPrimary':True},
            {'number':'5678', 'person':ppl[0], 'isPrimary':False},
            {'number':'7890', 'person':ppl[0], 'isPrimary':False}
            ])
    calls = inserts(DerivePhoneCall, [
            {'minutes': 10, 'city':'Chicago', 'phone':phones[0]},
            {'minutes': 20, 'city':'Detroit', 'phone':phones[0]},
            {'minutes': 10, 'city':'Chicago', 'phone':phones[1]},
            {'minutes': 15, 'city':'Chicago', 'phone':phones[1]},
            ])


def test_oneFrom():
    luke = DerivePerson.byName('Luke')
    ph = DerivePhone.byNumber('1234')
    assert luke.primaryPhone == ph
    assert luke.primaryPhone == luke.phones.filter(DerivePhone.q.isPrimary==True).getOne()
    #assert luke.secondaryPhones == phones[1]
    
def test_filterFrom():
    luke = DerivePerson.byName('Luke')
    assert list(luke.secondaryPhones) == list(luke.phones.filter(DerivePhone.q.isPrimary==False))
    
def test_aggregates():
    ph1 = DerivePhone.byNumber('1234')
    ph2 = DerivePhone.byNumber('5678')
    assert ph1.totalMinutes == ph1.calls.sum(DerivePhoneCall.q.minutes)
    #assert ph1.totalLocalMinutes == ph1.calls.filter(DerivePhone.sum(DerivePhoneCall.q.minutes)
    
