import json

def json_serializer(v):
    return json.dumps(v).encode('utf-8')

def json_deserializer(v):
    return json.loads(v.decode('utf-8'))