flatten <- function (project, table, tier) {
  
  ## 0. Load dependencies and define parameters ##################################
  library(jsonlite)
  library(tools)
  library(bigrquery)
  library(dplyr)
  library(glue)
  library(httr)
  
  # Load helper functions
  source(file.path("r", "connect_analytics_helper_functions.R"))
  source(file.path("r", "json_schema_to_csv.R"))
  source(file.path("r", "generate_flattening_query.R"))
  source(file.path("r", "update_scheduled_query.R"))
  
  print(glue("Flattening {table} now..."))
  # Generate file names given table name
  dataset      <- "Connect"
  query_folder <- "query_configs"
  table_folder <- file.path(query_folder, table, tier)
  query_config <- jsonlite::fromJSON(file.path(table_folder, "query-config.json"))
  
  # Create temporary directory to store temp files in
  tmp_folder <- file.path(table_folder, "tmp")
  if (!dir.exists(tmp_folder)) {dir.create(tmp_folder)}
  
  
  ## 1. Get schema from BQ as a JSON (only format option) ########################
  
  # Name schema files
  schema_json <- file.path(tmp_folder, paste0(table, "-schema.json"))
  schema_csv  <- file.path(tmp_folder, paste0(table, "-schema.csv"))
  
  # Construct bq_table object
  bq_table     <- bq_table(project, dataset, table)
  
  # Retrive the metadata for the table (a list containing the schema)
  metadata     <- bigrquery::bq_table_meta(bq_table)
  
  # Simplify the structure of the schema so that it matches that returned by bq CLI
  schema       <- metadata$schema$fields
  
  # Write the schema to a JSON string
  schema_as_json <- jsonlite::toJSON(schema, pretty = TRUE, auto_unbox = TRUE)
  write(schema_as_json, schema_json)
  
  
  ## 2. Convert schema from JSON to CSV ##########################################
  
  # Filter some names and types from csv and export to csv. Return a dataframe.
  names_to_filter <- c("__key__", "__error__", "__has_error__",
                       "treeJSON", "allEmails", "allPhoneNo", 
                       "query", "unverifiedSeen", "undefined", 
                       "utm_id", "utm_source", "verifiedSeen", 
                       "firstSurveyCompletedSeen", "COMPLETED", 
                       "COMPLETED_TS", "sha", "entity", "569151507")
  types_to_filter <- c("RECORD")
  schema_df <- json_schema_to_csv(schema_json, schema_csv, names_to_filter, types_to_filter)
  
  
  ## 3. Filter variables into "lists" and "variables" files based on data structure ####
  # Variables in the schema are filtered into 2 groups and sends them to different
  # file formats that can be used by the query generator: 
  # (1) variables with JSON arrays (aka lists) --> tmp/*-lists-from-schema.json 
  # (2) variables with single values           --> tmp/*-variables-from-schema.csv
  
  # Generate the query from the schema in the form of a set of variables.csv 
  # and lists.json files.
  out_json <- file.path(tmp_folder, paste0(table, "-lists-from-schema.json"))
  out_csv  <- file.path(tmp_folder, paste0(table, "-variables-from-schema.csv"))
  
  # Get variables and lists files from schema
  full_table_name <- glue("{dataset}.{table}")
  filter_vars_from_schema(project, full_table_name, schema_csv, out_csv, out_json)
  
  
  ## 4. Combine json and csv files prior to generating query #####################
  
  # Now that we have a new query in the form of variables.csv and lists.json files,
  # we need to combine the new json file with the old json file. This is useful
  # because it allows variables from the original files to remain even if they are
  # not present in the BQ tables yet. It also allows manual changes to be preserved.
  
  # -lists.json file from parent folder
  old_lists_json <- file.path(table_folder, paste0(table, "-lists.json"))
  
  # name combined json file
  union_lists_json <- file.path(tmp_folder, paste0(table, "-lists-union.json"))
  
  # Get union of old_lists_json and out_json
  union_json_str <- get_union_of_json_arrays(old_lists_json, out_json, union_lists_json)
  
  # We also need to combine the variables.csv file that was generated from the
  # schema with the original, manually generated variables.csv file. This code will
  # exclude any duplicates.
  
  # Get union of variable files
  old_vars_csv   <- file.path(table_folder, paste0(table, "-variables.csv"))
  union_vars_csv <- file.path(tmp_folder,   paste0(table, "-vars-union.csv"))
  union_vars_csv
  union_var_chr_array <- get_union_of_variable_csv(old_vars_csv, out_csv, union_vars_csv)
  
  
  ## 5. Deal with exceptions #####################################################
  
  # Some of the variables that contain JSON arrays in BQ only have single values in
  # the array. So we can exclude them from the "lists.json" file and put them in the
  # "variables.csv" instead. So a variable that appears as "d_CID: [178420302]" will
  # not be flattened as "d_cid_d_178420302" and have a value of 1. It will instead
  # be flatted as d_cid and will return a value of [178420302]. I added this
  # exception process to accommodate some issues Kelsey was having with variables
  # from Quest with unexpected data structure. We're ignoring the CID: d_742186726
  # which identifies stray tubes.
  
  # Define exceptions
  exceptions <- c(178420302, 958239616, 353358909, 104430631, 742186726, 496539718)
  
  # Get a list of variables with arrays containing "exception CID"
  vars_w_exceptions <- get_vars_with_specified_vals(union_lists_json, 
                                                    exceptions, 
                                                    max_array_len = 2) 
  
  # Remove list of variables with exceptions from the data set
  new_lists_json <- file.path(tmp_folder, paste0(table, "-lists-union-mod.json"))
  updated_variables <- remove_vars_from_json(union_lists_json, 
                                             vars_w_exceptions,
                                             output_filename = new_lists_json) 
  
  # Add list of variables with exceptions to \*-variables\*.csv
  new_vars_csv <- file.path(tmp_folder, paste0(table, "-variables-union-mod.csv"))
  add_novel_vars_to_csv(vars_w_exceptions, union_vars_csv, out_file = new_vars_csv)
  
  # Get list of variables from csv file that are of type RECORD or mode REPEATED in
  # the schema. There shouldn't be any left at this point, but just in case...
  vars_to_remove <- get_record_and_repeated_vars(project, table, schema_csv, new_vars_csv)
  
  # Remove these variables from the new csv file
  if ( length(vars_to_remove ) != 0) { remove_vars_from_csv(new_vars_csv, vars_to_remove) }
  
  
  ## 6. Replace original lists.json and variables.csv files in the parent directory
  
  # Replace original files in parent directory with new ones
  file.rename(new_vars_csv,   old_vars_csv)
  file.rename(new_lists_json, old_lists_json)
  
  
  ## 7. Clean up intermediate files from tmp folder ##############################
  
  # They don't need to be saved or version controlled so might as well avoid
  # clutter.
  
  # Delete intermediate files from tmp folder. 
  unlink(tmp_folder, recursive=TRUE)
  
  
  ## 8. Generate query ##########################################################
  query <-
     generate_flattening_query(
       source_table      = query_config$data_source[tier],
       destination_table = glue("{project}.{query_config$output_table}"),
       variable_csv      = old_vars_csv,
       arrays_json       = old_lists_json,
       table_description = query_config$table_description,
       filter_statement  = query_config$filter_statement,
       var_prefix        = query_config$variable_prefix,
       order_statement   = query_config$order_statement
     )
  
  query_file_path <- 
    file.path(table_folder, paste0(query_config$output_table, "-", tier, ".sql")) %>% 
    print()
  fileConn <- file(query_file_path)
  writeLines(query, fileConn)
  close(fileConn)
  
  
  ## 9. Update scheduled queries in GCP programatically --------------------------
  
  # Extract the resource name
  resource_name <- query_config$resource_name[tier]
  
  # Extract the transfer configuration ID
  transfer_config_id <- tail(strsplit(resource_name[[tier]], "/")[[1]], 1)
  
  update_scheduled_query(project, query, transfer_config_id)
  
  # If run_now is set to True, run the query in BigQuery, check the status, 
  # wait for the query to complete, and then confirm that the status is "DONE"
  if (run_now ==TRUE) {
    job <- bq_perform_query(query, project = project, billing = project)
    bq_job_status(job)
    bq_job_wait(job)
    bq_job_status(job)
  }
}