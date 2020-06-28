# -*- coding: utf-8 -*-
"""
@author: Dipson Shahi
@email: dipsonshahi14@gmail.com
"""
import psycopg2

class Connection():
    
    USER = None
    PASS = None
    HOST = None
    PORT = None
    DATABASE = None
    
    conn = None
    
    def __init__(self, USER, PASS, HOST, PORT, DATABASE ):
        
        self.USER = USER
        self.PASS = PASS
        self.HOST = HOST
        self.PORT = PORT
        self.DATABASE = DATABASE
        
        try:
          cnx = cnx = psycopg2.connect(user=self.USER
                                              , password=self.PASS
                                              , host=self.HOST
                                              , port = self.PORT
                                              , database=self.DATABASE)
          
          cnx.autocommit = False
          
          print("Connected to Database!")
          self.conn = cnx
          
        except psycopg2.Error as err:
            
          if err.errno == psycopg2.errorcodes.INVALID_AUTHORIZATION_SPECIFICATION:
            print("Something is wrong with your user name or password")
          elif err.errno == psycopg2.errorcodes.CONNECTION_DOES_NOT_EXIST:
            print("Database does not exist")
          else:
            print(err)
        
    def execQuery(self, Query_params, params):
        cursor = self.conn.cursor()
        cursor.execute(Query_params, params)
    
    def execQueryArray( self, Query_params, paramsArray ):
        cursor = self.conn.cursor()
        cursor.executemany(Query_params, paramsArray)
    
    def commit(self):
        """ To confirm the inserts, you have to end with a commit """
        self.conn.commit()
        #print("commit")
    
    
    def execQuerySimple(self, query):
        cursor = self.conn.cursor()
        cursor.execute(query)
        self.conn.commit()


"""
con = Connection()
con.commit()
"""











