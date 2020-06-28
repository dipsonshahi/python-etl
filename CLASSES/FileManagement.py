# -*- coding: utf-8 -*-
"""
pip install pyunpack
pip install patool

# use this for macos
pip install rarfile
brew install unrar

@author: Dipson Shahi
"""
import os
import urllib.request
from pyunpack import Archive
from rarfile import RarFile

class FileManagement():
    
    INPUT_PATH = None
    OUTPUT_CSV_FILE = None
    PROCESSED_FILE_PATH = None
    
    DOWNLOADED_FILE_NAME = "order-sample.rar"
        
    files_xml = list()
        
    def __init__( self):
        print("File Manager created")
        
    def getFilesXMLFromOrigin( self, PATH ):
        """ This function stores in the received list the location of all the
         files that are in the PATH that receives by parameter """
        self.files_xml.clear()
        dictionary = None
        self.files_xml = list()
        for root, dirs, files in os.walk( PATH ):
            for file in files:
                if file.endswith(".xml") | file.endswith(".XML"):
                        
                    dictionary = { "INPUT_PATH": root
                                    , "FILE": file }
                    
                    self.files_xml.append(dictionary)
                    
        return self.files_xml
              

    def create_output_path_if_not_exists(self, PATH):
        if ( not os.path.exists( PATH ) ):
            os.makedirs( PATH )

    def downloadOrdersRarURL(self, ORDERS_DOWNLOAD_URL, DOWNLOAD_PATH ):
        print('Beginning file download with urllib2...')
        
        self.create_output_path_if_not_exists(DOWNLOAD_PATH)

        urllib.request.urlretrieve( ORDERS_DOWNLOAD_URL, DOWNLOAD_PATH + self.DOWNLOADED_FILE_NAME )

    def unRarFileExport(self, DOWNLOAD_PATH, EXPORT_PATH ):
        
        self.create_output_path_if_not_exists(EXPORT_PATH)
        
        """Archive( DOWNLOAD_PATH + self.DOWNLOADED_FILE_NAME ).extractall( EXPORT_PATH )"""
        with RarFile( DOWNLOAD_PATH + self.DOWNLOADED_FILE_NAME ) as rf:
            rf.extractall( EXPORT_PATH )
        
        

"""
ORDERS_DOWNLOAD_URL = "https://github.com/dipsonshahi/python-etl/blob/master/order-sample.rar"
DOWNLOAD_PATH = "/Users/dipson/Desktop/DataManagement/PYTHON/DOWNLOAD/"
EXPORT_PATH = "/Users/dipson/Desktop/DataManagement/PYTHON/DATASOURCE/"
fileManagement = FileManagement()
fileManagement.downloadOrdersRarURL( ORDERS_DOWNLOAD_URL, DOWNLOAD_PATH )
fileManagement.unRarFileExport( DOWNLOAD_PATH, EXPORT_PATH )

"""
                
"""   
Test method that returns a list of files 
fileManagement = FileManagement()
list = fileManagement.getFilesXMLFromOrigin( "/Users/dipson/Desktop/DataManagement/PYTHON/DATASOURCE/" )

"""












