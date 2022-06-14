from pymongo import MongoClient
from dotenv import load_dotenv
import jsonlines
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
        if value['input']:
            if value['type'] == 'select':
                fields[key] = 'categorical'
            elif key not in {'lat', 'lon'}:
                fields[key] = 'numeric'
    
    return fields


def retrieve_segment_data(bounding_box, view_name, fields):
    database_name = 'turf'
    collection_name = f'{view_name}_collection'
    
    latitudes = [coordinate[1] for coordinate in bounding_box]
    longitudes = [coordinate[0] for coordinate in bounding_box]
    match_query = {
        'LAT': {'$gte': min(latitudes), '$lte': max(latitudes)},
        'LON': {'$gte': min(longitudes), '$lte': max(longitudes)},
    }
    
    facet_query = {}
    check_query = {'$or': []}
    for field, field_type in fields.items():
        field_name = field.upper()
        if field_type == 'categorical':
            facet_query[field] = [
                {'$match': {field_name: {'$ne': None}}},
                {'$group': {'_id': f'${field_name}', 'count': {'$sum': 1}}},
                {'$group': {'_id': None, 'values': {'$push': {'k': '$_id', 'v': '$count'}}}},
                {'$replaceRoot': {'newRoot': {'$arrayToObject': '$values'}}},
            ]
            check_query['$or'].append({field: {'$ne': []}})
        else:
            facet_query[field] = [
                {'$match': {field_name: {'$ne': None}}},
                {'$group': {'_id': None, 'max': {'$max': f'${field_name}'}, 'min': {'$min': f'${field_name}'}}},
                {'$project': {'_id': 0, 'max': 1, 'min': 1}}
            ]
    
    pipeline = [{'$match': match_query}, {'$facet': facet_query}, {'$match': check_query}]
    with MongoConnectionManager(database_name, collection_name) as collection:
        data = list(collection.aggregate(pipeline=pipeline, allowDiskUse=True))
    
    if data:
        data = data[0]
    else:
        return {'nodata': 'no data in this grid'}
    
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
    
    # writer_collection = f'{view_name}_clusters'
    # with MongoConnectionManager(database_name, writer_collection) as collection:
    #     collection.insert_one(document)
    
    with jsonlines.open(f'{view_name}-grid-data.jsonl', 'a') as writer:
        writer.write(document)
    
    return document


def prepare_data():
    f = open('static/geojson/processed-small-grids.geojson', 'r')
    data = json.load(f)
    processed = []
    view_name = "property"
    fields = get_filter_fields(view_name)
    count = 0
    
    for feature in data['features']:
        print("Processing", count)
        bbox = feature['geometry']['coordinates'][0]
        doc = retrieve_segment_data(bbox, view_name, fields)
        if 'nodata' in doc:
            print(f'no data in grid {count}')
        else:
            processed.append(doc)
        count += 1
    
    processed_dump = json.dumps(processed)
    with open(f'static/geojson/processed-{view_name}.json', 'w') as writer:
        writer.write(processed_dump)


if __name__ == '__main__':
    load_dotenv()
    redis_instance = get_redis_instance()
    prepare_data()
