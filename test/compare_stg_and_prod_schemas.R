library(bigrquery)
library(dplyr)
library(glue)

# GCP Project IDs
tiers <- c("stg", "prod")
projects <- list(dev  = "nih-nci-dceg-connect-dev",
                 stg  = "nih-nci-dceg-connect-stg-5519",
                 prod = "nih-nci-dceg-connect-prod-6d04")
dataset = "Connect"

tables <-
  c(
    "bioSurvey_v1",
    "biospecimen",
    "boxes",
    "clinicalBioSurvey_v1",
    "covid19Survey_v1",
    "menstrualSurvey_v1",
    "module1_v1",
    "module1_v2",
    "module2_v1",
    "module2_v2",
    "module3_v1",
    "module4_v1",
    "notifications",
    "participants"
  )

schema_list = list()

for (tier in tiers) {
  for (table in tables) {
    # Select project
    project <- toString(projects[tier])
    
    # Construct file name
    # Construct file name
    dir_path <- glue::glue("test/schemas/{tier}")
    if (!dir.exists(dir_path)) {
      dir.create(dir_path, recursive = TRUE)
    }
    filename <- glue::glue("{dir_path}/{dataset}.{table}")
    print(glue("Running {filename}"))
    
    # Construct bq_table object
    bq_table <- bq_table(project, dataset, table)
    
    # Retrieve the metadata for the table (a list containing the schema)
    metadata <- bigrquery::bq_table_meta(bq_table)
    
    # Simplify the structure of the schema so that it matches that returned by bq CLI
    schema <- metadata$schema$fields
    
    # Write the schema to a JSON string
    schema_json <-  paste0(filename, ".json")
    schema_as_json <-
      jsonlite::toJSON(schema, pretty = TRUE, auto_unbox = TRUE)
    write(schema_as_json, file(schema_json))
    
    # Filter some names and types from csv and export to csv. Return a dataframe.
    names_to_filter <- c("__key__",
                         "__error__",
                         "__has_error__",
                         "treeJSON",
                         "allEmails",
                         "allPhoneNo")
    types_to_filter <- c("RECORD")
    source("gcp_query_tools/json_schema_to_csv.R")
    schema_csv <- paste0(filename, ".csv")
    schema_df <-
      json_schema_to_csv(schema_json, schema_csv, names_to_filter, types_to_filter)
    
    # Store the schema_df in the schema_list
    schema_list[[paste(tier, table)]] <- schema_df
    
    # Generate the query from the schema in the form of a set of variables.csv and lists.json files.
    out_json <- paste0(filename, "-lists-from-schema.json")
    out_csv  <- paste0(filename, "-variables-from-schema.csv")
    
    # Get variables and lists files from schema
    filter_vars_from_schema(project, 
                            paste(dataset, table, sep= "."),
                            schema_csv,
                            out_csv,
                            out_json,
                            output_file_type="json")
  }
  
}



for (table in tables) {
  
  stg_schema  <- schema_list[[paste('stg',  table)]]
  prod_schema <- schema_list[[paste('prod', table)]]
  
  # Check if they are identical
  is_identical = identical(stg_schema, prod_schema)
  print(glue("{table} schemas identical? {is_identical}"))
  
  # Check if they are 
  only_in_stg  <- setdiff(names(stg_schema),  names(prod_schema))
  only_in_prod <- setdiff(names(prod_schema), names(stg_schema) )
  
  print(only_in_stg)
  print(only_in_prod)
  
}

