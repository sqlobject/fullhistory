from SQLObject import *

__connection__ = SQLiteConnection('prueba.db')


class Place(SQLObject):
        name = StringCol()

class Move(SQLObject):
        from_ = ForeignKey('Place')
        to    = ForeignKey('Place')
