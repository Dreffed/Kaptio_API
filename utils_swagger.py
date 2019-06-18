
from swagger_parser import SwaggerParser
import yaml

#parser = SwaggerParser(swagger_path='http://ktapi-staging.herokuapp.com/static/swagger.yaml')  # Init with file
data = {}
yaml_file = r"C:\Users\dgloyncox\Downloads\swagger.yaml"
with open(yaml_file, 'r') as stream:
    try:
        data = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        print(exc)

for k,v in data.items():
    print(k, type(v), len(v))
    if isinstance(v, dict):
        for k_i,v_i in data.get(k,{}).items():
            print('\t',k_i)
            if isinstance(v_i, dict):
                for k_s,v_s in v_i.items():
                    print('\t'*2, k_s, v_s)

    elif isinstance(v, list):
        for i in v:
            print('\t',i)
    else:
        print('\t',v)




