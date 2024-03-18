'''
Description: This program reads a text file with a SQL query and a config file with info 
about a scheduled query in BQ and updates the text of the scheduled query using
the Big Query API. Modified from example: 
  https://cloud.google.com/bigquery/docs/scheduling-queries#python_4

Author: Jake Peters
Date: Feb 2023

Usage:

  


Notes about requirements:

Must install bigquery_datatransfer: https://pypi.org/project/google-cloud-bigquery-datatransfer/
    $ pip3 install google-cloud-bigquery-datatransfer

Must authenticate: https://googleapis.dev/python/google-api-core/latest/auth.html
If you’re developing locally, the easiest way to authenticate is using the Google Cloud SDK:
    $ gcloud auth application-default login

Note that this command generates credentials for client libraries. To authenticate the CLI itself, use:
    $ gcloud auth login

Previously, gcloud auth login was used for both use cases. If your gcloud installation does not support the new command, please update it:
    $ gcloud components update

To set project use 
    $ gcloud config set project PROJECT_ID
Examples:
    $ gcloud config set project nih-nci-dceg-connect-prod-6d04
    $ gcloud config set project nih-nci-dceg-connect-stg-5519
    $ gcloud config set project nih-nci-dceg-connect-dev

API Documentation: https://cloud.google.com/bigquery/docs/reference/datatransfer/rest/v1/projects.locations.transferConfigs/patch
'''

import sys
from google.cloud import bigquery_datatransfer, bigquery
from google.protobuf import field_mask_pb2
import json

# Get arguments, starting at second argument. The first argument is name of this script for some reason.
args = sys.argv[1:]
print(args)

# Get arguments: project_id, module_folder, run_now
run_now = False
if  len(args) < 2: raise Exception("Must be at least 2 arguments. Arguments: project_id, module_folder, run_now (optional, bool is false by default).") 
elif len(args) > 3: raise Exception("Too many arguments. Arguments: project_id, module_folder, run_now (optional, bool is false by default).")
elif len(args) ==3: run_now = args[2]

# Set project and tier
project = args[0]
if 'dev' in project: tier = 'dev'
elif 'stg' in project: tier = 'stg'
elif 'prod' in project: tier = 'prod'
else: raise Exception("Project argument is invalid.") 

# Load configuration file
module_folder = args[1]
query_config_file = module_folder + "query-config.json"
print(query_config_file)
with open(query_config_file) as f: query_config = json.load(f)

# Get query string from file
query_file = module_folder + query_config["query_name"] + "-" + tier + ".sql"
with open(query_file) as file: query_str = file.read()
dotdotdot = "\n\t.\n\t.\n\t.\n"
print("query_file: " + query_file)
print("query_str: \n\n" + query_str[0:200] + dotdotdot)

# Format query str with """ quotes """ and put in query_params
query_string = """q""".replace("q", query_str)
query_params = {"query": query_string}

# TODO: Put transfer configs in config file for each tier and write a function to get transfer_config_name from GCP.
# TODO: Write function to get transfer_config_name given the project_ID and the display_name
# Update transfer configurations
transfer_config_name = query_config["resource_name"][tier]
print("transfer_config_name: " + transfer_config_name)
transfer_client = bigquery_datatransfer.DataTransferServiceClient()
transfer_config = bigquery_datatransfer.TransferConfig(name=transfer_config_name)
transfer_config.params = query_params

transfer_config = transfer_client.update_transfer_config(
    {
        "transfer_config": transfer_config,
        # IMPORTANT NOTE: Anything that you want to update must be in the paths list!!
        # i.e., paths=["transfer_config_name","params","ETCETERA"]
        "update_mask": field_mask_pb2.FieldMask(paths=["params"])
    }
)
print(f"Updated config: '{transfer_config.name}'")
print(f"Display name: '{transfer_config.display_name}'")

if run_now:
    client = bigquery.Client(project=project)
    query_job = client.query(query_string)  # Make an API request.
    print("Running query.. Check GCP to confirm when complete.")
    # query_job.result()  # Wait for the job to complete.
    


# Useful code:

# # Print all scheduled queries from BQ
# parent = transfer_client.common_project_path(project)
# configs = transfer_client.list_transfer_configs(parent=parent)
# for config in configs:
#     print(f"\tDisp_Name: {config.display_name}, ID: {config.name}, Schedule: {config.schedule}")

# Get current project
# client = bigquery.Client(project=project)
# print('GCP project set to: ', client.project)
