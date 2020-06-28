"""
@author: Dipson Shahi
"""
from xml.etree import ElementTree as ET
import pandas as pd
from datetime import datetime

class ParserXML():
    """ Initialize the lists that will contain the records of the tables / CSV  """
    list_headers = list()
    list_orders = list()
    list_order_details = list()
    
    pd_list_headers = None
    pd_list_orders = None
    pd_list_order_details = None
    
    FILE = None
    INPUT_PATH = None

    def __init__(self):
        print("Parser created")

    def parseXML( self, FILE, INPUT_PATH ):
        """ Load the input XML file into the xroot variable """
        
        self.FILE = FILE
        self.INPUT_PATH = INPUT_PATH
        
        FILE_PARSER = INPUT_PATH + FILE
        
        xtree = ET.parse( FILE_PARSER )
        xroot = xtree.getroot()

        """ Defining the functions  """
        def getValue(value):
            """ This function is to receive the value of the XML objects
            in case it is not None """
            if (value is not None):
                return value.text
            else:
                None

        def funcion_list_headers(self, xroot):
            """ This function extracts the data from the file header from the XML """
            
            self.list_headers.clear()
            start_date = xroot.find('header/start_date').text
            end_date = xroot.find('header/end_date').text
            page = xroot.find('header/page').text
            DateInsert = str(datetime.now())

            # Create a dictionary with the values
            dictionary = {  "from_date": start_date, 
                            "end_date": end_date, 
                            "page": page, 
                            "DateInsert": DateInsert
                         }
            # Add this dictionary to the list. You only have one record
            self.list_headers.append(dictionary)
            

        def funcion_list_orders( self, xroot ):
            """ Lists all the records that are in the 'order' tag"""
            
            self.list_orders.clear()
            for node in xroot.findall('order'):
                order_id = node.attrib.get('id')
                client_id = node.find('client_id')
                client = node.find('client')
                date = node.find('date')
                discount_price = node.find('discount_price')
                DateInsert = str(datetime.now())

                self.list_orders.append({  "order_id": order_id, 
                                            "client_id" : getValue(client_id), 
                                            "client" : getValue(client), 
                                            "date" : getValue(date), 
                                            "discount_price" : getValue(discount_price), 
                                            "DateInsert": DateInsert
                                        })

        def funcion_list_order_details( self, xroot ):
            
            self.list_order_details.clear()
            for node in xroot.findall('order'):
                order_id = node.attrib.get('id')
                client_id = node.find('client_id')
                client = node.find('client')
                date = node.find('date')
                discount_price = node.find('discount_price')
                DateInsert = str(datetime.now())
                
                for node_2 in node.findall('details/line'):
                    product_id = node_2.attrib.get('product_id')
                    color = node_2.find('color')
                    price = node_2.find('price')
                    units = node_2.find('units')
    
                    self.list_pedidos_detalles.append({ "order_id": order_id, 
                                                        "client_id" : getValue(client_id), 
                                                        "client" : getValue(client), 
                                                        "date" : getValue(date), 
                                                        "discount_price" : getValue(discount_price), 
                                                        "DateInsert": DateInsert, 
                                                        "product_id" : product_id, 
                                                        "color" : getValue(color), 
                                                        "price" : getValue(price), 
                                                        "units" : getValue(units)
                                                    })
        

        """ Parse the XML file to get the values of the lists """
        funcion_list_headers(self, xroot )
        funcion_list_orders(self, xroot )
        funcion_list_order_details(self, xroot )

        """ Creating dataframes from dictionary lists works best with Pandas """
        self.pd_list_headers = pd.DataFrame( self.list_headers )
        self.pd_list_orders = pd.DataFrame( self.list_orders )
        self.pd_list_order_details = pd.DataFrame( self.list_order_details )


    def insertRowsToDB(self, connection):
        
        self.connection = connection
        def insert_list_headers(self):
            sql_insert = """ insert into headers ( page, start_date, end_date, DateInsert ) 
                            values ( %s, %s, %s, %s ) """
                            
            params_array = (    self.list_header[0]["page"], 
                                self.list_header[0]["start_date"], 
                                self.list_header[0]["end_date"], 
                                self.list_header[0]["DateInsert"]
                            )
            
            print(params_array)
            
            self.connection.execQuery( Query_params = sql_insert, params = params_array )
        
        def insert_list_orders(self):
            sql_insert = """ insert into orders ( id, client_id, client, date, discount_price, DateInsert )
                            values ( %s, %s, %s, %s, %s, %s ) """
        
            params_array = []
            for order in self.list_orders:
                params_array.append( (
                                        order["order_id"], 
                                        order["client_id"], 
                                        order["client"], 
                                        order["date"], 
                                        order["discount_price"], 
                                        order["DateInsert"]
                                    ) )
                
            self.connection.execQueryArray( Query_params = sql_insert, paramsArray = params_array )
                
        def insert_list_order_details(self):
            sql_insert = """ insert into order_details ( id, client_id, client, date, discount_price, DateInsert
                                                    , product_id, color, price, units )
                            values ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s ) """
            params_array = []
            for order in self.list_order_details:
                params_array.append( (
                                        order["order_id"]
                                        , order["client_id"]
                                        , order["client"]
                                        , order["date"]
                                        , order["discount_price"]
                                        , order["DateInsert"]
                                        , order["product_id"]
                                        , order["color"]
                                        , order["price"]
                                        , order["units"]
                                        )
                                    )
            self.connection.execQueryArray( Query_params = sql_insert, paramsArray = params_array )
            
        insert_list_headers(self)
        insert_list_orders(self)
        insert_list_order_details(self)
        
        self.connection.commit()
        

 
"""
create table headers ( 
	page varchar(100), 
    start_date date, 
    end_date date, 
    DateInsert timestamp
);

create table orders ( 
	id integer, 
    client_id integer, 
    client varchar(100), 
    date timestamp, 
    discount_price float, 
    DateInsert timestamp
);

create table order_details ( 
	id integer, 
    client_id integer,
	client varchar(100),
	date timestamp, 
	discount_price float, 
	DateInsert timestamp,
	product_id integer, 
	color varchar(50),
	price float, 
	units integer,  
);
 
"""
