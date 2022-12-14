# File:        connect_analytics_helper_functions.R
# Description: This file contains functions that are useful for generating 
#              queries directly from a bigquery table schema (in tabular form).
#              It also has functions that facilitate combining multiple queries
#              together by taking the union of a *-variables.csv or *-lists.json
#              file. This is useful if you want to combine an automated query 
#              with a manually edited query.
# Author:      Jake Peters
# Date:        October 2022

get_union_of_json_arrays <- function(json_A, json_B, json_C) { # nolint
  # Description:
  #   This function combines two *-list.json files (json_A & json_B) so that
  #   json_C has all of the unique variable names present in json_A and/or 
  #   json_B with no duplicates. The array of values assigned to each variable 
  #   name in json_C has all of the unique values present in json_A and/or 
  #   json_B with no duplicates.
  # Inputs:
  #   json_A: a json file that you would like to combine with json_B
  #   json_B: a json file that you would like to combine with json_A
  #   json_C: the name of the output json file that you would like to generate
  # Returns:
  #   list_C_as_JSON_str: a json string that is written to file json_C that is 
  #   the union of json_A and json_B
  # Side Effects:
  #   Writes list_C_as_JSON_str to a file with a name given by json_C.
    
  require(jsonlite)
  require(tools)
  
  # If a and b are files convert files to R lists
  if (tools::file_ext(A) == "json") {
    list_A <- jsonlite::fromJSON(json_A)
    list_B <- jsonlite::fromJSON(json_B)
  }
  list_C <- list()
  
  # Get variable names unique to A, unique to B or common to both
  var_names_uniq_to_A   <- setdiff(names(list_A), names(list_B))
  var_names_uniq_to_B   <- setdiff(names(list_B), names(list_A))
  var_names_in_A_and_B  <- intersect(names(list_A), names(list_B))
  
  # Add variables/array values common to both A and B to C
  for (variable_name in var_names_in_A_and_B) {
    
    # Get arrays assigned to list_A$variable_name and list_B$variable_name
    array_A <- eval(parse(text = paste0("list_A$", variable_name)))
    array_B <- eval(parse(text = paste0("list_B$", variable_name)))
    
    # Get array of unique values found in A and B combined
    array_C <- union(array_A, array_B)
    
    # Add array to list_C under listC$variable_name
    eval(parse(text = (paste0("list_C$", variable_name, " <- array_C"))))
  }
  
  # Add variables unique to list A to list C
  for (variable_name in var_names_uniq_to_A) {
    array <- eval(parse(text = paste0("list_A$", variable_name)))
    eval(parse(text = (
      paste0("list_C$", variable_name, " <- array")
    )))
  }
  
  # Add variables unique to list B to list C
  for (variable_name in var_names_uniq_to_B) {
    array <- eval(parse(text = paste0("list_B$", variable_name)))
    eval(parse(text = (paste0("list_C$", variable_name, " <- array"))))
  }
  
  # Sort variables in list_C
  list_C <- lapply(list_C, sort)         # Sort CIDs within JSON arrays
  list_C <- list_C[order(names(list_C))] # Sort variables
  
  # Convert list C to JSON string
  list_C_as_JSON_str <- jsonlite::prettify(jsonlite::toJSON(list_C), indent = 4)
  
  # Export JSON string to file
  write(list_C_as_JSON_str, json_C)
  
  return(list_C_as_JSON_str)
}

get_union_of_variable_csv <- function(A, B, C) {
  # Description:
  #   This function combines two *-list.csv files (A & B) so that
  #   C has all of the unique variable names present in A and/or 
  #   B with no duplicates. 
  # Inputs:
  #   A: a csv file that you would like to combine with B
  #   B: a csv file that you would like to combine with A
  #   C: the name of the output csv file that you would like to generate
  # Returns:
  #   C_char_array: a character array containing all of the variables of A 
  #                 and/or B that is written to file C.
  # Side Effects:
  #   Writes C_char_array to a csv file with a name given by C.
  
  require(tools)
  
  # If a and b are csv or txt files load them as character arrays, otherwise 
  # assume that they are character arrays
  if (tools::file_ext(A) == "csv" | tools::file_ext(A) == "txt") {
    A <- readLines(A)
    B <- readLines(B)
  }
  
  # Get union of A and B and sort as character array
  C_char_array <- sort(union(A, B))
  
  # Write combined character array to new file
  write(C_char_array, C)
  
  return(C_char_array)
}

get_vars_with_specified_vals <- function(json, vals, max_array_len = Inf) {
  # Description:
  #   This function 
  # Inputs:
  #   json: 
  #   vals: 
  #   max_array_len: 
  # Returns:
  #   vars_w_vals: 
  # Side Effects:
  #   
  
  require(jsonlite)
  require(tools)
  
  # If json is file convert file to R lists, otherwise assume already are
  if (tools::file_ext(A) == "json") {
    var_list <- jsonlite::fromJSON(json)
  }
  
  var_names <- names(var_list)
  vars_with_vals <- c()
  for (variable_name in var_names) {
    array <- eval(parse(text = paste0("var_list$", variable_name)))
    
    # Skip this variable if it has an array length of more than 2
    if (length(array) > max_array_len) { next }
    
    # Check if all values in the array are vals
    all_vals_are_vals <- TRUE # Initialize as TRUE
    for (cid in array) {      # Change to False if any are vals
      if (! cid %in% vals) { all_vals_are_vals <- FALSE }
    }
    
    # If all values in the array are vals, add them to the list
    if (all_vals_are_vals) {
      vars_with_vals <- append(vars_with_vals, variable_name)
    } 
  }
  
  return(vars_with_vals)
}

remove_vars_from_json <- 
  function(json, vars, output_filename = NULL) {
    # Description:
    #   This function removes variables (listed in vars) from a json string.
    #   The json string can be an input or be contained with in an input file. 
    #   
    # Inputs:
    #   json: A json string or file
    #   vars: a list of variables to be removed
    #   write_to_file: A filename to write the resulting json string to. 
    #                  If FALSE, no file is generated.
    # Returns:
    #   vars_w_vals: 
    # Side Effects:
    #   
    
  require(jsonlite)
  require(tools)

  # If json is file convert file to R lists, otherwise assume already are
  is_file <- FALSE
  if (tools::file_ext(A) == "json") {
    var_list <- jsonlite::fromJSON(json)
    is_file <- TRUE
  }
  
  # Remove variables from list
  for (var in vars) {
    var_list <- var_list[names(var_list) != var]
  }
  
  if (! is.null(output_filename)) {
    write(jsonlite::prettify(jsonlite::toJSON(var_list), indent = 4), 
          output_filename)
  }
  
  return(var_list)
  
}

add_novel_vars_to_csv <- function(vars, in_file, out_file = NULL) {
  # Description:
  #   This function combines a list of new variables to a *-variables.csv file.
  # Inputs:
  #   vars: an array of variable names to be added
  #   in_file: a *-variables.csv file 
  #   out_file: the name of the new file to be generated with the full list of 
  #             variables. If NULL, in_file is overwriten with the full list.
  # Returns:
  #   combined_vars_list: A character array of variables including those from 
  #                       the input file and vars.
  # Side Effects:
  #   Writes combined_vars_list to a file.
  
  # Generate list of variables in the in_file by reading each line as a value
  vars_from_file <- readLines(in_file)
  
  # Initialize a list of variables to append
  combined_vars_list <- vars_from_file
  
  for (var in vars) {
    # If the variable is not in the list of variables from the file
    if (! var %in% vars_from_file) {
      # Add the variable to the list
      combined_vars_list <- append(combined_vars_list, var)
    }
  }
  
  # Sort variables 
  combined_vars_list <- sort(combined_vars_list)
  
  # If there is no specified out_file, set it to be the same as the in_file
  if (is.null(out_file)) {
    output_file = in_file
  } 
  
  # Write new variable list (including originals and added variables) to file
  writeLines(combined_vars_list, out_file)
  
  #return(vars_to_export)
}

get_list_of_unique_responses <- function(var_name, project, table){
  # Description:
  #   This function queries a variable (that returns a json array in GCP) 
  #   specified by var_name within a given project and table, and generates a 
  #   list of unique values or responses that appear in the json arrays for that 
  #   variable.
  # Inputs:
  #   var_name: The name of the variable of interest as a string.
  #   project:  The GCP project ID as a string.
  #   table:    The name of the table as a string (e.g., "Database.table_name")
  # Returns:
  #   unique_responses: A character array of the unique values or responses 
  #                     returned by the variable across all rows of the table.
  # TODO: Take an array of variables as an input and write one query for all 
  #       of them. There is a 10MB minimum charge, so combining running one 
  #       query for all of the variables would be more cost efficient.
  
  # Query data for this variable
  tab_path <- paste0("`", project, ".", table, "`") # Format table path for SQL
  bq_query <- paste("SELECT", var_name, "AS var FROM", tab_path,  
                    "WHERE",  var_name, "IS NOT NULL", sep=" ")
  bq_table <- bq_project_query(project, bq_query) # 
  df_var   <- bq_table_download(bq_table, bigint = "integer64")
  
  # Build list of unique responses for this variable
  unique_responses <- c()
  for (i in 1:nrow(df_var)){
    responses           <- df_var$var[[i]]
    responses_available <- length(responses) > 0
    if (responses_available){
      for (j in 1:length(responses)){
        response          <- responses[j]
        response_is_novel <- !(response %in% unique_responses)
        if (response_is_novel){
          unique_responses <- append(unique_responses, response)
        }
      }
    }
  }
  return(unique_responses)
}

filter_vars_from_schema <- function(project, table, schema, out_csv, out_json, 
                                    sheet=NULL, output_file_type = "json") {
  # Description
  #   This function filters the variables in the schema of a bigquery table by 
  #   type and puts them in a *-lists.json file if they return json arrays or a
  #   *-variables.csv file if they do not return json arrays. All variables 
  #   containing the string pattern "__key__" are excluded. For each variable 
  #   that returns a json array, a function called get_list_of_unique_responses
  #   queries bigquery to get a list of all possible responses/values that 
  #   are returned for that variable across all rows of the data set. This array
  #   of unique values is added to the appropriate json array in the 
  #   *-lists.json file.
  # Inputs
  #   project:  The GCP project ID as a string
  #   table:    The table name as a string, e.g., "Database.table"
  #   schema:   The schema of the table in the form of an excel sheet.
  #   out_csv:  The name of the output csv, e.g., "M1-variables.csv"
  #   out_json: The name of the output file, e.g., "M1-lists.json"
  #   sheet:    The sheet of the excel file to be referenced. Defaults to NULL.
  # Side Effects
  #   Writes a *-variables.csv and *-lists.json file.
  #
  # TODO Use JSON lite to generate json file from R list rather than using 
  # string concatenation to construct the json file.
  
  require("readxl")
  require("dplyr")
  require("bigrquery")
  require("jsonlite")
  
  billing <- project # Billing should be same as project
  bq_auth() # Authenticate with BigQuery (Note: project & billing used here)
  
  ## Get schema data for Version 1 and Version 2
  df <- read_excel(schema, sheet = sheet) # Module 1 Version 1 Schema
  
  # Make all columns factors so they can be used when filtering.
  # The "[]" keeps the dataframe structure.
  df[] <- lapply(df, factor) 
  
  ## Filter out RECORDS (variables with nested data) and keys
  df_filt <- df %>% filter(!grepl("__key__", fullname)) %>% filter(!grepl("RECORD", type))
  
  ## Get list of variables that do not repeat (destined for M2*-variables.csv)
  df_vars <- df_filt %>% filter(!grepl("REPEATED", mode))
  
  ## Get variables that do repeated (destined for M2*-lists.js)
  df_lists <- df_filt %>% filter(grepl("REPEATED", mode))
  
  ## Export variables to M2V*-variables.csv file for queryGenerator
  write.table(df_vars$fullname, file = out_csv, col.names = FALSE,
              row.names = FALSE, quote = FALSE, sep = ","
  )
  
  if (output_file_type == "js"){
    
    # Define strings to open and close M2V*-lists.js files
    opening_line <- "const pathToConceptIdList = {"
    closing_line <- "};"
    ending_line  <- "module.exports=pathToConceptIdList;"
    
    # Write lines
    write(opening_line, file = out_json, append = FALSE) # Overwrite existing file
    cnt <- 0
    for (var_name in df_lists$fullname) {
      cnt            <- cnt + 1
      responses_list <- get_list_of_unique_responses(var_name, project, table)
      responses_str  <- paste(responses_list, collapse = ", ")
      var_line       <- paste0("'", var_name, "': [", responses_str, "],")
      write(var_line, file = out_json, append = TRUE) # Do not overwrite, append
    }
    write(closing_line, file = out_json, append = TRUE)
    write(ending_line,  file = out_json, append = TRUE)
    
    json_data <- NULL
    
  } else if (output_file_type == "json") {
    
    # Write lines 
    array_list = list()
    for (var_name in df_lists$fullname) {
      responses_as_chr_array <- get_list_of_unique_responses(var_name, 
                                                             project, 
                                                             table)
      array_list_field_name  <- paste0("array_list", "$", var_name)
      print(array_list_field_name)
      assign(array_list_field_name, responses_as_chr_array)
      array_list
    }
    json_data <- toJSON(array_list,pretty=TRUE,auto_unbox=TRUE)
    write(json_data, out_json)
    
  }
  
  output_list            <- list()
  output_list$variables  <- df_vars$fullname
  output_list$array_json <- json_data
  return(output_list)
}


#TODO function for loading schema in json form and flattening to csv or xlsx.