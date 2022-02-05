import pymysql as sql
import time
import numpy as np

booleanDB = False



class Crud_mysql:  # MongoDB Model for ToDo CRUD Implementation
    def __init__(self, config):   # Fetchs the MongoDB, by making use of Request Body        
        self.SQL_CONNECTION = sql.connect(
                            host=config['SERVER_URL'],
                            user=config['USER_NAME'],
                            passwd=config['PASSWORD'],   
                            database=config['DB'],
                            charset=config['CHARSET']                         
                            )   
        print("connection successed")
    def __del__(self):
        self.SQL_CONNECTION.close()
        print("connection closed")                    
   #  def connectionNodb(config):
                           
    ########## Create database
    def createDB(self, query):
        conn = self.SQL_CONNECTION  ## if database is not on MYSql, create database
        mycursor = conn.cursor()
        mycursor.execute(query)        
    
    ######### Create table
    def createTale(self, query):
        conn = self.SQL_CONNECTION
        mycursor=conn.cursor()#cursor() method create a cursor object  
        mycursor.execute(query)#Execute SQL Query to create a table into your database          
    
    ######## Insert Record
    
    def insert(self, query):
        conn = self.SQL_CONNECTION
        mycursor=conn.cursor()#cursor() method create a cursor object    
        try:  
           #Execute SQL Query to insert record  
           mycursor.execute(query)  
           conn.commit() # Commit is used for your changes in the database  
           print('Record inserted successfully...')   
        except:  
           # rollback used for if any error   
           conn.rollback()          
           
    def insertDataArray(self, query):
        conn = self.SQL_CONNECTION
        mycursor=conn.cursor()#cursor() method create a cursor object    
        try:  
           #Execute SQL Query to insert record               
#           print(index)
           mycursor.execute(query)                      
           conn.commit() # Commit is used for your changes in the database  
           #print('Record inserted successfully...')   
        except:  
           # rollback used for if any error   
           conn.rollback()                
    
    ######## Update Record
    def update(self, query):
        conn = self.SQL_CONNECTION
        mycursor=conn.cursor()#cursor() method create a cursor object  
        try:  
           mycursor.execute(query)
           conn.commit() # Commit is used for your changes in the database  
           print('Record updated successfully...')   
        except:   
           # rollback used for if any error  
           conn.rollback()  
           print("Update failed")        
    
    ######## Read Record
    def read(self , query):
        conn = self.SQL_CONNECTION
        mycursor=conn.cursor()#cursor() method create a cursor object  
        data_read = []
        try:  
           mycursor.execute(query)#Execute SQL Query to select all record   
           result=mycursor.fetchall() #fetches all the rows in a result set                                               
           for i in result:    
              #roll=i[0]  
              #name=i[1]  
              #marks=i[2]  
              data_read.append(i)
              #print(roll,name,marks)  
           print("Data read successed")
           return data_read
        except:   
           print('Error:Unable to fetch data.')               
    
    ######## Delete Record
    def delete(self, query):
        conn = self.SQL_CONNECTION
        mycursor=conn.cursor()#cursor() method create a cursor object   
        try:   
           mycursor.execute(query)#Execute SQL Query to detete a record   
           conn.commit() # Commit is used for your changes in the database  
           print('Record deleted successfully...')  
        except:  
           # rollback used for if any error  
           conn.rollback()  
        
        
################################################## End function



sqlConfig = {
        "SERVER_URL":"localhost",
        "USER_NAME":"root",
        "PASSWORD":"",
        "DB":"testMysql",
        "CHARSET":"utf8mb4"        
        } 
 
#query = "create database testMysql"
#createDB(conn, query)        

crud = Crud_mysql(sqlConfig)
  
        
        
############################# Selecting data  amount of data is like 50 10^3, 10^4, 10^5     

################################  Insert, Read, Remove test 50 10^3, 10^4, 10^5

selectTest0 = []
selectTest1 = []
selectTest2 = []
selectTest3 = []


removeTest0 = []
removeTest1 = []
removeTest2 = []
removeTest3 = []

InsertTest0 = []


readData = []
######################### Read remove insert 50 datas
for ii in range(5):
########################## first read 50 datas
    for i in range(1):          
        query = "SELECT * FROM train_full LIMIT 100"
        start_time = time.time()
        readData = crud.read(query)  
        end_time = time.time()
        total_time = end_time-start_time    
        selectTest0.append(total_time)
        
    ####################################### second remove 50 datas    
    
    for i in range(1):
        query = "DELETE FROM train_full LIMIT 100"
        start_time = time.time()
        crud.delete(query)  
        end_time = time.time()
        total_time = end_time-start_time    
        removeTest0.append(total_time)
    
    ####################################### Third insert 50 datas   
    #print(len(readData))
    index = 0
    InsertTest = []
    for i3 in readData:      
        index += 1
        #print(index)
        #print(i3)        
        str2 = []
        for j in i3:           
            #print(j)            
            j_type =type(j)     
            #print(j_type) 
            if j == None:
                j_1 = "'null'"
                str2.append(j_1)
                continue
            if type(j)==str:
                j = "'"+str(j)+"'"            
                str2.append(j)  
                continue
            str2.append(j)    
        str3 = ','.join(map(str,str2))            
        query = "INSERT INTO train_full VALUES"+ "(" + str3 + ")"   
        #print(query)         
        start_time = time.time()
        crud.insertDataArray(query)
        end_time = time.time()
        total_time = end_time-start_time    
        InsertTest.append(total_time)

    InsertTest0.append(sum(InsertTest))       
