#def json_schema_to_csv(json_file_name, csv_file_name, 
#                       filter='{"name": ["__key__", "__error__", "__has_error__", "treeJSON", "allEmails", "allPhoneNo"], "type": ["RECORD"]}'
#                       ):

import sys
import json
import csv

# Get arguments, starting at second argument. The first argument is json_schema_to_csv_b.py for some reason.
args = sys.argv[1:]
print("args[0]: " + args[0])


# Set a default filter. Variables with these properties will be excluded from output. Filter argument overides this.
filter = json.loads('{"name": ["__key__", "__error__", "__has_error__", "treeJSON", "allEmails", "allPhoneNo"], "type": ["RECORD"]}')

# Get arguments: json_file_name, csv_file_name, filter (optional)
if len(args) < 2:
    print("Must be at least 2 arguments. Arguments: json_file_name, csv_file_name, filter (optional)")
    exit()
elif len(args) > 3:
    print("args[2]: " + args[2])
    print("Too many arguments. Arguments: json_file_name, csv_file_name, filter (optional)")
    exit()
elif len(args) == 3:
    # The filter argument must be a JSON string. Convert it to dictionary.
    filter = json.loads(args[2])

json_file_name = args[0]
csv_file_name = args[1]

# Open json file
f = open(json_file_name)

# JSON object as a python dictionary
data_queue = json.load(f)

# Get keys, aka future column names
keys = data_queue[0].keys() # should be 'description', 'mode', 'name', 'type', and maybe 'fileds'

data_out = []
idx = 0
while data_queue:
    data_obj = data_queue.pop(0) # get/remove first obj from queue
    
    if 'fields' in data_obj:
        # add each child to queue to with concatenated parent/child name
        parent_name = data_obj['name']
        for child_obj in data_obj['fields']:
            child_obj['name'] = parent_name + '.' + child_obj['name']
            data_queue.append(child_obj)
            
    elif 'fields' not in data_obj:
        
        # Check if filter rules apply
        pass_filter = True
        for filter_key in filter.keys():
            if filter_key in data_obj and \
                any(pattern in data_obj[filter_key] for pattern in filter[filter_key]):
                pass_filter = False
                break
        if not pass_filter:
            continue    
        
        # does not have children, dump data to file
        data_out.append(data_obj)
        
with open(csv_file_name, 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(keys)
    
    for i in data_out:
        li = []
        for k in keys:
            if k in i:
                li.append(i[k])
            else:
                li.append(None)
        writer.writerow(li)
print("generated " + csv_file_name)
            
#json_schema_to_csv('module2_v2-schema.json', 'module2_v2-schema.csv')             
