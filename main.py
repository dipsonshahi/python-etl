"""
@author: Dipson Shahi
"""

from CLASSES.ParserXML import ParserXML
from CLASSES.LogTraceability import LogTraceability
from CLASSES.Connection import Connection
from CLASSES.FileManagement import FileManagement
from datetime import datetime
import json

"""
- Start the process of parsing the list of XML files found in the list: files_xml
- It connects to the database and inserts everything that happens and calculates times, etc. in the log table.
"""
#DEFINE THE ENVIRONMENT TO COLLECT THE VALUES FOUND IN THE config.json
ENVIRONMENT = "PRODUCTION"      #PRODUCTION | DEVELOPMENT | TEST | LOCAL


# Get access variables
with open('/Users/dipson/Downloads/python_parserXML/config.json', 'r') as file:
    config = json.load(file)

connection = Connection( USER = config[ENVIRONMENT]['USER']
                        , PASS = config[ENVIRONMENT]['PASS']
                        , HOST = config[ENVIRONMENT]['HOST']
                        , PORT = config[ENVIRONMENT]['PORT']
                        , DATABASE = config[ENVIRONMENT]['DATABASE'] )


#Create traceability to record everything that is happening in the control table
logTraceability = LogTraceability( connection = connection
                                  , ETL_MASTER = "PARSER XML TESTING")

process_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S") # This date is a key field for traceability
logTraceability.iniStatusActivity( FILE = "N/A"
                                  , DESCRIPTION = "PROCESSED"
                                  , DATE = process_date
                                  ,  STATUS = 0 )

activity_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S") # This date is a key field for traceability
fileManagement = FileManagement()


process_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S") # This date is a key field for traceability
logTraceability.iniStatusActivity( FILE = "N/A"
                                  , DESCRIPTION = "DOWNLOAD & UNRAR"
                                  , DATE = process_date
                                  ,  STATUS = 0 )
fileManagement.downloadOrdersRarURL( ORDERS_DOWNLOAD_URL = config[ENVIRONMENT]['ORDERS_DOWNLOAD_URL']
                                        , DOWNLOAD_PATH = config[ENVIRONMENT]['DOWNLOAD_PATH'] )

fileManagement.unRarFileExport( DOWNLOAD_PATH = config[ENVIRONMENT]['DOWNLOAD_PATH']
                                    , EXPORT_PATH = config[ENVIRONMENT]['EXPORT_PATH'] )

logTraceability.updateStatusActivity( FILE = "N/A"
                                        , DESCRIPTION = "DOWNLOAD & UNRAR"
                                        , DATE = process_date
                                        , STATUS = 1 )


#get a list of all the XML files inside the PATH that are sent to it by parameter
files_xml = fileManagement.getFilesXMLFromOrigin( PATH = config[ENVIRONMENT]['EXPORT_PATH'] )
parserXML = ParserXML()
for inputFile in files_xml:
    
    INPUT_PATH = inputFile["INPUT_PATH"]
    FILE = inputFile["FILE"]
    
    activity_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S") # This date is a key field for traceability
    
    try:
        
        logTraceability.iniStatusActivity( FILE = FILE
                                          , DESCRIPTION = "ParserXML"
                                          , DATE = activity_date
                                          ,  STATUS = 0 )
        print("Parsing the file: " + FILE)
        
        parserXML.parseXML( FILE, INPUT_PATH )
        parserXML.insertRowsToDB( connection )
        
    except:
        
        logTraceability.updateStatusActivity( FILE = FILE
                                             , DESCRIPTION = "ParserXML"
                                             ,  DATE = activity_date
                                             ,  STATUS = -1 )
        
    else:
        
        logTraceability.updateStatusActivity( FILE = FILE
                                             , DESCRIPTION = "ParserXML"
                                             ,  DATE = activity_date
                                             ,  STATUS = 1 )
        
    
### for inputFile in files_xml: 
print("Bye!")    
logTraceability.updateStatusActivity( FILE = "N/A"
                                     , DESCRIPTION = "PROCESSED"
                                     ,  DATE = process_date
                                     ,  STATUS = 1 )
