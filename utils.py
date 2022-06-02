from pymongo import MongoClient
import redis
import json
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


def get_redis_instance():
    redis_instance = redis.Redis(
        host=os.environ.get('REDIS_HOST'),
        port=os.environ.get('REDIS_PORT'),
        password=os.environ.get('REDIS_AUTH')
    )
    return redis_instance


def get_filter_fields(section_name):
    filter_key = f'{section_name}_skeleton'
    redis_instance = get_redis_instance()
    filters = json.loads(redis_instance.get(filter_key))['filters']
    
    fields = {}
    for key, value in filters.items():
        if value['type'] == 'select':
            fields[key] = 'categorical'
        elif key not in {'lat', 'lon'}:
            fields[key] = 'numeric'
    
    return fields


def retrieve_segment_data(bounding_box, view_name):
    database_name = 'turf'
    collection_name = f'{view_name}_collection'
    
    latitudes = [coordinate[1] for coordinate in bounding_box]
    longitudes = [coordinate[0] for coordinate in bounding_box]
    match_query = {
        'LAT': {'$gte': min(latitudes), '$lte': max(latitudes)},
        'LON': {'$gte': min(longitudes), '$lte': max(longitudes)},
    }
    
    fields = get_filter_fields(view_name)
    facet_query = {}
    for field, field_type in fields.items():
        field_name = field.upper()
        if field_type == 'categorical':
            facet_query[field] = [
                {'$match': {field_name: {'$ne': None}}},
                {'$group': {'_id': f'${field_name}', 'count': {'$sum': 1}}},
                {'$group': {'_id': None, 'values': {'$push': {'k': '$_id', 'v': '$count'}}}},
                {'$replaceRoot': {'newRoot': {'$arrayToObject': '$values'}}},
            ]
        else:
            facet_query[field] = [
                {'$match': {field_name: {'$ne': None}}},
                {'$group': {'_id': None, 'max': {'$max': f'${field_name}'}, 'min': {'$min': f'${field_name}'}}},
                {'$project': {'_id': 0, 'max': 1, 'min': 1}}
            ]
    
    pipeline = [{'$match': match_query}, {'$facet': facet_query}]
    with MongoConnectionManager(database_name, collection_name) as collection:
        data = list(collection.aggregate(pipeline=pipeline, allowDiskUse=True))[0]
    
    properties = {}
    for field in fields:
        if data[field]:
            properties[field] = data[field][0]
        else:
            properties[field] = {}
    
    document = {
        'properties': properties,
        'geometry': {
            'type': 'Polygon',
            'coordinates': bounding_box,
        }
    }
    
    return document
