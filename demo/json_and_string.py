import json

data = {'key': 'value', 'number': 42}

str_data = str(data)
print(type(str_data))
print(str_data)

json_data = json.dumps(data).encode('utf-8')
print(type(json_data))
print(json_data)

decoded_data_str = json.loads(str_data)
print(type(decoded_data_str))
print(decoded_data_str)

decoded_data_json = json.loads(json_data.decode('utf-8'))
print(type(decoded_data_json))
print(decoded_data_json)