import json

def read_json(file_name) :
    f = open(file_name)
    data = json.load(f)
    f.close()
    return data