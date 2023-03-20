# JSON schema to CSV
#
# Convert GCP table schema from JSON file to CSV file. 
# Implemented as a breadth-first tree traversal.
# 
# Written by: Jake Peters
# Date: February, 2023
#
# parameters: 
#             json_file_name  Name of the GCP schema as JSON file.
#             csv_file_name   Name of the CSV file to schema to.
#             filter          json string with names/types to filter out
#
# returns:    A dictionary with the filtered schema

# example: 

# Filter some names and types from csv and export to csv. Return the filtered
# schema as a dictionary.
   
# filter='{"name": ["__key__", "__error__", "__has_error__", "treeJSON", "allEmails", "allPhoneNo"],"type": ["RECORD"]}' 
# schema_dict = json_schema_to_csv("schema.json", "schema.csv", filter)
                    
def json_schema_to_csv(json_file_name, 
                       csv_file_name, 
                       filter='{"name": ["__key__", "__error__", "__has_error__", "treeJSON", "allEmails", "allPhoneNo"], "type": ["RECORD"]}'
                       ):
    import sys
    import json
    import csv
    
    filter = json.loads(filter)
    
    # Open json file
    f = open(json_file_name)

    # JSON object as a python dictionary
    data_queue = json.load(f)

    # Get keys, aka future column names
    keys = data_queue[0].keys() # should be 'description', 'mode', 'name', 'type', and maybe 'fileds'

    data_out = []
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
    
    return data_out
