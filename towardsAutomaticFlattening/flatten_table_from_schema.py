# Import dependencies
import treelib
import json

# Import nested json data file
json_file_name = "recruitment_participants_schema.json"
f = open(json_file_name) 
data_list = json.load(f) # Load json data as a dictionary
print('\n\ndata_list = \n\n')
[print(i) for i in data_list]

# Display part of dictionary
dot = '\n\t\t\t\t.\n'
n = 20 # number of items to display from beginning and end of dictionary
print('\n\ndictionary: \n\n', data_list[0:n], dot, dot, dot, data_list[-n:-1], '\n\n')

# Generate tree of variables
tree = treelib.Tree()  # initialize tree
tree.create_node(tag='Top', identifier='Top')
check = [] # Initialize queue of items to be checked as a list

# Iterate through each level to check which children belong to which parent
# Main Loop
for item in data_list:
  
    tree.create_node(tag=item['name'], 
                         identifier=item['name'], 
                         data={'type': item['type']},
                         parent='Top') # Must change this if deeper in tree using queue!
    item_has_children = 'fields' in item and item['type'] == 'RECORD'
    if item_has_children:
      print(item['name'], 'has children')
      parent_name = item['name']
      # Child Loop
      for child in item['fields']:
        
        tree.create_node(tag=child['name'], 
                         identifier=child['name'], 
                         data={'type': child['type']},
                         parent=parent_name)
    else:
      print(item['name'], 'does not have children')
      print('Continuing to main loop.')
      continue

