from pymongo import MongoClient
import os


class MongoConnectionManager:
    def __init__(self, database, collection):
        self.client = MongoClient(os.environ.get('MONGO_URI'))
        self.database = database
        self.collection = collection
    
    def __enter__(self):
        self.database = self.client[self.database]
        self.collection = self.database[self.collection]
        return self.collection
    
    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.client.close()


def retrieve_segment_data(bounding_box, view_name):
    database_name = 'turf'
    collection_name = f'{view_name}_collection'
    pipeline = []
