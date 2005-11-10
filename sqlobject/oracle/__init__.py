from sqlobject.dbconnection import registerConnection

def builder():
    import oracleconnection
    return oracleconnection.OracleConnection

def isSupported():
    try:
        import cx_Oracle
##         import DCOracle2
    except ImportError:
        return False
    return True

registerConnection(['oracle'], builder, isSupported)
