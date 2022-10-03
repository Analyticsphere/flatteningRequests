#!/bin/bash

# Author:       Jake Peters
# Date:         September 2022

# Description:  This shell script takes 3 arguments: (1) gcp_project,
#               (2) database.table and (3) schema_json_output_filename.
#               It then downloads the schema of the specified table from the
#               gcp project and saves it to the working directory.

# Notes:        This can be easily used from within an R script that already
#               deals with authentification using bq_auth().
#               Example:
#                 arg1 <- ""nih-nci-dceg-connect-stg-5519""
#                 arg2 <- "Connect.participants"
#                 arg3 <- "participants_schempa.json"
#                 system("get_table_schema_as_json.sh arg1 arg2 arg3")

if [ $# -ne 3 ]; then
        echo "specify 3 command line arguments:/ <gcp_project> <database.table> <schema_json_output_filename>"
		exit 1
fi

# Convert schema json file name to string
schema_json_fid="'$3'"

# Write table schema to formatted json file using gcloud command.
bq show --schema --format=prettyjson $1:$2 > $3
echo -e "\n\nSchema saved as ${3}.\n"

# Display first L lines of schema to check if it looks ok.
L=15
echo -e "\nPreview of the first and last ${L} lines of ${3}:\n"
head -$L $3
echo -e "\n\t\t.\n\t\t.\n\t\t.\n"
tail -$L $3
