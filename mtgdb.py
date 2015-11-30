import MySQLdb
from db_params import params  

def connect():
    return MySQLdb.connect(user=params['user'],passwd=params['passwd'],db=params['db'])

def serialize(el):
    if type(el) == type(u''):
        el = el.encode('utf-8')
    if type(el) == type(''):
        return "'{}'".format(el.replace("'", "\\'"))
    if el == None:
        return 'NULL'
    return str(el)

def names_from_data_table(data_table):
    return list(set().union(*[row.keys() for row in data_table]))

def insert_statement(table_name, data_table):
    statement = 'INSERT INTO {} '.format(table_name)
    if not len(data_table):
        statement += '() VALUES ()'
        return statement
    
    names = names_from_data_table(data_table)
    statement += '({}) VALUES '.format(str.join(',', names))

    values = []
    for row in data_table:
        content = str.join(',', [serialize(row.get(name, None)) for name in names])
        values.append('({})'.format(content))

    statement += str.join(',', values)
    return statement

class Cursor:
    def __init__(self):
        self.__db = connect()
        self.__cursor = self.__db.cursor()
    
    def insert(self, table_name, data_table):
        statement = insert_statement(table_name, data_table)
        print 'executing statement: {}'.format(statement[:75] + ' ... ' + statement[-75:])
        message = '{} rows added to {}'.format(self.__cursor.execute(statement), table_name)
        print message
        return self

    def execute(self, query):
        self.__cursor.execute(query)
        return self.__cursor.fetchall()

    def close(self, commit=True):
        if commit:
            self.__db.commit()
        self.__cursor.close()
        self.__db.close()
