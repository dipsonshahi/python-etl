# -*- coding: utf-8 -*-
"""
@author: Dipson Shahi
"""
from datetime import datetime

class LogTraceability():
    
    IDPROCESS = 0
    connection = None
    ETL_MASTER = None
    
    def __init__(self, connection, ETL_MASTER):
        self.IDPROCESS = self.getIdProcess()
        self.connection = connection
        self.ETL_MASTER = ETL_MASTER
        print("Log Traceability: " + self.IDPROCESS)
        
    def getIdProcess(self):
        return datetime.now().strftime("%Y%m%d%H%M%S")
        
    def iniStatusActivity( self, FILE, DESCRIPTION, DATE, STATUS, CANT_RECORDS = None ):
        """ Insert a new record in the control table """
        
        sql_insert = """insert into ctl_activity_process ( id_process, etl_master, activity, file, status, start_date, cant_row ) VALUES """
        sql_insert += """ ( %s, %s, %s, %s, %s, %s, %s ) """
        params = ( self.IDPROCESS, self.ETL_MASTER, DESCRIPTION, FILE, STATUS, DATE, CANT_RECORDS )
        self.connection.execQuery( sql_insert, params )
        self.connection.commit()
    
    def updateStatusActivity( self, FILE, DESCRIPTION, DATE, STATUS ):
        """ Modify status and end date of inserted record """
        
        sql_update = """ update ctl_activity_process set 
                            	status = %s
                                , end_date = %s
                        where id_process = %s
                        	and activity = %s
                        	and file = %s
                        	and start_date = %s
                    """
        params = ( STATUS, datetime.now(), self.IDPROCESS, DESCRIPTION, FILE, DATE )
        self.connection.execQuery( sql_update, params )
        self.connection.commit()
        
    def getIdprocess(self):
        return self.IDPROCESS
    

    
"""
logTraceability = LogTraceability( connection = connection
                                  , ETL_MASTER = "PARSER XML TESTING")

process_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S") # This date is a key field for traceability
logTraceability.iniStatusActivity( FILE = "N/A"
                                  , DESCRIPTION = "PROCESSED"
                                  , DATE = process_date
                                  , STATUS = 0 )

process_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S") # This date is a key field for traceability
logTraceability.iniStatusActivity( FILE = "DIPSON SHAHI"
                                  , DESCRIPTION = "PARSER XML"
                                  , DATE = process_date
                                  , STATUS = 0 )

process_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S") # This date is a key field for traceability
logTraceability.updateStatusActivity( FILE = "DIPSON SHAHI"
                                      , DESCRIPTION = "PARSER XML"
                                      , DATE = process_date
                                      , STATUS = 1 )    


process_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S") # This date is a key field for traceability
logTraceability.updateStatusActivity( FILE = "N/A"
                                      , DESCRIPTION = "PROCESSED"
                                      , DATE = process_date
                                      , STATUS = 1 )      
"""

"""
CREATE TABLE ctl_activity_process (
  etl_master varchar(50),
  id_process bigint,
  file varchar(100),
  activity varchar(100),
  status int,
  start_date timestamp,
  end_date timestamp,
  cant_row int
);
        
"""
    