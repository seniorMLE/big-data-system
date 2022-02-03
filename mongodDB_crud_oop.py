import time
from pymongo import MongoClient



# initialized the Flask APP


class Crud:  # MongoDB Model for ToDo CRUD Implementation
    def __init__(self, data):   # Fetchs the MongoDB, by making use of Request Body
        self.client = MongoClient("mongodb://localhost:27017/")
        database = data['database']
        collection = data['collection']
        cursor = self.client[database]
        self.collection = cursor[collection]
        self.data = data

    def insert_data(self, data):    # Create - (1) explained in next section
        new_document = data
        response = self.collection.insert_one(new_document)
        output = {'Status': 'Successfully Inserted',
                  'Document_ID': str(response.inserted_id)}
        return output
    
    def insert_data_many(self, data):    # Create - (1) explained in next section
        new_document = data
        response = self.collection.insert_many(new_document)
        output = {'Status': 'Successfully Inserted',
                  'Document_ID': str(response.inserted_ids)}
        return output

    def read_all(self):                 # Read - (2) explained in next section
        documents = self.collection.find()
        output = [{item: data[item] for item in data } for data in documents]
        return output
    
    def read_multi(self, query):                 # Read - (2) explained in next section
        documents = self.collection.find(query)
        output = [{item: data[item] for item in data if item != '_id'} for data in documents]
        return output
    
    def read_limit(self,number):
        documents = self.collection.find().limit(number)
        output = [{item: data[item] for item in data } for data in documents]
        return output
    def read_single(self, query):                 # Read - (2) explained in next section
        documents = self.collection.find_one(query)
        output = documents
        return output

    def update_single(self,data):          # Update - (3) explained in next section
        filter = data['Filter']
        present_data = self.collection.find_one(filter)        
        updated_data = {"$set": data['DataToBeUpdated']}
        response = self.collection.update_one(present_data, updated_data)
        output = {'Status': 'Successfully Updated' if response.modified_count > 0 else "Nothing was updated."}
        return output
    
    def update_many(self,data):          # Update - (3) explained in next section
        filter = data['Filter']
        updated_data = {"$set": data['DataToBeUpdated']}
        response = self.collection.update_many(filter, updated_data)
        output = {'Status': 'Successfully Updated' if response.modified_count > 0 else "Nothing was updated."}
        return output

    def delete_single(self, data):    # Delete - (4) explained in next section
        filter = data
        response = self.collection.delete_one(filter)
        output = {'Status': 'Successfully Deleted' if response.deleted_count > 0 else "Document not found."}
        return output
    
    def delete_many(self, data):    # Delete - (4) explained in next section
        filter = data
        response = self.collection.delete_one(filter)
        output = {'Status': 'Successfully Deleted' if response.deleted_count > 0 else "Document not found."}
        return output
    def delete_byid(self, data):        
        result = self.collection.delete_one({"_id":data})
        
        #output = [{item: data[item] for item in data if item != '_id'} for data in documents]
        #return output
        
    def delete_all(self ):    # Delete        
        self.collection.drop({})
        



data = {"database":"testDB",
        "collection":"train_full"        
        }
documents=[{"Name":"Roshan","Roll No":159,"Branch":"CSE"},
           {"Name":"Rahim","Roll No":155,"Branch":"CSE"},
           {"Name":"Ronak","Roll No":156,"Branch":"CSE"}]
document={"Name":"Raj",
          "Roll No":  153,
          "Branch": "CSE"}

query_single = {"Name":"Raj"}
query_multi={"Branch":"CSE"}

# create databasse and delete all the collections and insert documents and read single {"Name":"Raj"}
test = Crud(data)  #create database and collection
#test.insert_data(document)
#readData = test.read_all()
#print(data)
#for i in data:
#    O_id = i['_id']
#print(O_id)
#print(test.delete_byid(O_id))

#readData = test.read_all()
insertTest = []
readTest = []
removeTest0 = []
removeTest = []
updateTest0 = []
updateTest = []


for i in range(1):        
    ####################### Rading data
    
    start_time = time.time()
    readData = test.read_limit(100000)
    end_time = time.time()
    total_time = end_time-start_time    
    readTest.append(total_time)

    ###########################  Deleting  data
    for O_id in readData:
    	#print(O_id)
    	start_time = time.time()
    	test.delete_byid(O_id['_id'])
    	end_time = time.time()
    	total_time = end_time-start_time
    	removeTest0.append(total_time)  
    removeTest.append(sum(removeTest0))  
    removeTest0 = []
    
    ################################## Insert many data test
    
    start_time = time.time()
    test.insert_data_many(readData)
    end_time = time.time()
    total_time = end_time-start_time
    insertTest.append(total_time)

    for O_id in readData:        
        #print(O_id['_id'])
        will_update_data = {"Filter":O_id['_id'], "DataToBeUpdated":{"Name":"ECE"}}
        start_time = time.time()
        test.update_single(will_update_data)
        end_time = time.time()
        total_time = end_time-start_time
        updateTest0.append(total_time)
    updateTest.append(sum(updateTest0))
    updateTest0 = []

print("updateTest: "+ str(updateTest))
#print(insertTest)
#print(removeTest)
#print(readTest)
####################### Read Remove Insert


















