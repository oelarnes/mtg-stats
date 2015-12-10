import MySQLdb
from db_params import params  
from datetime import datetime

def connect():
    return MySQLdb.connect(user=params['user'],passwd=params['passwd'],db=params['db'])

def serialize(el):
    if type(el) is unicode:
        el = el.encode('utf-8')
    if type(el) is str:
        return "'{}'".format(el.replace("'", "\\'"))
    if el == None:
        return 'NULL'
    if type(el) is datetime:
        return "'{}'".format(el.isoformat()[:10])
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
        self.execute(statement)
        return self

    def execute(self, statement):
        print 'executing statement: {}'.format(statement)
        self.__cursor.execute(statement)
        return self.__cursor.fetchall()

    def close(self, commit=True):
        if commit:
            self.__db.commit()
        self.__cursor.close()
        self.__db.close()
