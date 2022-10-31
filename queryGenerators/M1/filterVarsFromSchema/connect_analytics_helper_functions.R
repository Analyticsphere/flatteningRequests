# File:       getVarsFromSchema.R
# Decription: Read Module 1 Schema from excel file. Filter variables and export
#             to M1-variables.csv and M1-lists.js files.
# Author:     Jake Peters
# Date:       October 2022

get_union_of_json_arrays <- function(json_A, json_B, json_C) {
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
  function(json, vars, write_to_file = FALSE, output_filename = NULL) {
    
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
  
  # Generate list of variables in the in_file by reading each line as a value
  vars_from_file <- readLines(in_file)
  
  # Initialize a list of variables to append
  vars_to_export <- vars_from_file
  
  for (var in vars) {
    # If the variable is not in the list of variables from the file
    if (! var %in% vars_from_file) {
      # Add the variable to the list
      vars_to_export <- append(vars_to_export, var)
    }
  }
  
  # Sort variables 
  vars_to_export <- sort(vars_to_export)
  
  # If there is no specified out_file, set it to be the same as the in_file
  if (is.null(out_file)) {
    output_file = in_file
  } 
  
  # Write new variable list (including originals and added variables) to file
  writeLines(vars_to_export, out_file)
  
}


# Define function to get list of responses for variables returning JSON arrays
get_list_of_unique_responses <- function(var_name, project, table){
  # Get a list of all unique answers that appear in the arrays of responses 
  # for this variable.
  
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
    # print(paste0("length responses: ", length(responses)))
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