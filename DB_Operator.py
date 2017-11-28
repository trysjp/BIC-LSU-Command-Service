from __future__ import print_function

import sqlite3

class SQL_DB_Operator(object):

    def __init__(self):
    
        pass
    
    # end of function __init__
    
    def query_single(self):
    
        pass
    
    # end of function query_single
    
    def query_multiple(self):
    
        pass
    
    # end of function query_multiple
    
    def query_repetitive(self):
    
        pass
    
    # end of function query_repetitive

# end of class DB_Session

class SQLite_DB_Operator(SQL_DB_Operator):

    def __init__(self, db_path):
    
        self.connection = \
            sqlite3.connect(db_path, check_same_thread=False)
        self.connection.execute('pragma foreign_keys=ON')
        self.connection.execute('pragma journal_mode=WAL')
        self.connection.isolation_level = None
        self.cursor = self.connection.cursor()
    
    # end of function __init__
    
    def __del__(self):
    
        self.cursor.close()
        self.connection.close()
    
    # end of function __del__
    
    def query_single(self, 
            query_string, 
            parameters = None):
    
        if parameters:
            #print("w/ para")
            self.cursor.execute(
                query_string, parameters)
        else:
            #print("w/o para")
            self.cursor.execute(
                query_string)
                    
        return self.cursor.fetchall()
    
    # end of function query_single
    
    def transaction_begin(self):
    
        self.cursor.execute("BEGIN TRANSACTION");
    
    # end of function transaction_begin
    
    def transaction_commit(self):
    
        self.cursor.execute("COMMIT");
    
    # end of function transaction_commit
    
    def transaction_rollback(self):
    
        self.cursor.execute("ROLLBACK");
    
    # end of function transaction_rollback
    
    def query_multiple(self, 
            query_string, 
            parameters = None):
    
        if parameters:
            self.cursor.executescript(
                query_string, parameters)
        else:
            self.cursor.executescript(
                query_string)
               
        return self.cursor.fetchall()
    
    # end of function query_multiple
    
    def query_repetitive(self, 
            query_string, 
            parameters):
    
        self.cursor.executemany(
            query_string, parameters)
            
        return self.cursor.fetchall()
    
    # end of function query_repetitive
    
    def commit(self):
    
        return self.connection.commit()
    
    # end of function commit
    
    def displayAllTables(self):
    
        table_name_records = \
            self.cursor.execute(
                'SELECT name FROM sqlite_master WHERE type = \'table\'')
                
        table_name_list = []
        
        for one_table_name_record in table_name_records:
            
            table_name_list.append(str(one_table_name_record[0]))
            
        # end of loop for
        
        for one_table_name in table_name_list:
        
            print(one_table_name)
            records = \
                self.cursor.execute(
                    'SELECT * FROM ' + one_table_name)
            for one_record in records:
                print(str(one_record))
        
        # end of loop for
    
    # end of function displayAllTables

# end of class SQL_DB_Operator

class MySQL_DB_Operator(SQL_DB_Operator):

    def __init__(self, url, user, password):
    
        pass
    
    # end of function __init__

# end of class MySQL_DB_Operator
