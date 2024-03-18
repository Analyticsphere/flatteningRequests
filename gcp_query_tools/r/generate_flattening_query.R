#' Generate a SQL query to flatten a Connect4Cancer data set in Big Query.
#'
#' @param source_table The source table in project.database.table form. 
#' @param destination_table The destination table in database.table form.
#' @param variable_csv A csv file listing variables that do not have JSON arrays as values in the source table.
#' @param arrays_json A JSON file listing variables that have JSON arrays as values in the source table.
#' @param filter_statement A SQL "WHERE" clause to filter query with. i.e., "WHERE uid IS NOT NUL"
#' @param table_description Optional table description to be put under details tab in destination table in BQ.
#' @param output_file_path Path/file name of the query to be generated.
#'
#' @return A SQL query that can be run in BQ generate the flattened table.
#' @export
#'
#' @examples
#' destination_table <- 'FlatConnect.module2_v2_JP'
#' table_description <- 'this table is just a test'
#' source_table      <- 'nih-nci-dceg-connect-dev.Connect.module2_v2'
#' json_file         <- 'module2_v2-lists.json'
#' csv_file          <- 'module2_v2-variables.csv'
#' filter_statement  <- 'WHERE Connect_ID IS NOT NULL'
#' output_file_path  <- 'test.sql'
#' var_prefix        <- 'D_'
#' 
#' query <- generate_flattening_query(destination_table = destination_table,
#'                                    table_description = table_description,
#'                                    source_table      = source_table,
#'                                    variable_csv      = csv_file,
#'                                    arrays_json       = json_file,
#'                                    filter_statement  = filter_statement,
#'                                    output_file_path  = output_file_path,
#'                                    var_prefix        = var_prefix)
#' cat(query)

generate_flattening_query <- function(source_table,
                                      destination_table,
                                      variable_csv,
                                      arrays_json,
                                      filter_statement='',
                                      table_description='',
                                      var_prefix='D_',
                                      config_file='',
                                      output_file_path='',
                                      order_statement = '\nORDER BY\n\tConnect_ID') {
  
  arrays_to_be_flattened <- paste(readLines(arrays_json), collapse='\n')
  # Flatten each CID and array of responses to <D_ParentCID.D_ChildCID> format
  cids_from_lists        <- generate_paths_from_cid_list(arrays_json, var_prefix)
  # Read list of CIDS with no array of responses as a list
  cids_without_lists     <- readLines(variable_csv)
  # Combine the two lists
  variables              <- append(cids_from_lists, cids_without_lists) %>%
                            unique() %>%
                            sort()
    
  # Generate line of Cloud SQL code for each variable to be pasted into sql_body
  selects                <- generate_selects(variables)
  
  notes <- glue::glue(
  ' 
  -- FYI: This query was automatically generated using R code written by Jake 
  -- Peters. The code references a the source table schema and a configuration
  -- file specifying various parameters. This style of query was developed by 
  -- Warren Lu to flatten the Connect4Cancer datasets in 2022. See GitHub repo
  -- for further documentation.
  -- 
  -- Repository: https://github.com/Analyticsphere/flatteningRequests
  -- Relavent functions: generate_flattening_query.R
  -- 
  -- source_table: {source_table}
  -- destination table: {destination_table}
  ')

  
  # The body of the SQL query is a long string with %s placeholders to sprintf
  # string variables into.
  sql_body <-
'%s -- notes
    
----- User-defined JavaScript functions used in BigQuery -----
CREATE TEMP FUNCTION
  handleRow(input_row STRING)
  RETURNS STRING
  LANGUAGE js AS r"""
  
  function getNestedObjectValue(obj, pathString) {
    function getNestedObjectValueFromPathArray(obj, pathArray) {
      let currKey = pathArray[0];
      if (
        typeof obj[currKey] === "undefined" || obj[currKey] === null ||
        pathArray.length === 1
      )
        return obj[currKey];
      return getNestedObjectValueFromPathArray(obj[currKey], pathArray.slice(1));
    }
    return getNestedObjectValueFromPathArray(obj, pathString.split("."));
  }
  
  function setNestedObjectValue(obj, pathString, value) {
    function setNestedObjectValueFromPathArray(obj, pathArray, value) {
      let currKey = pathArray[0];
      if (pathArray.length === 1) {
        obj[currKey] = value;
        return;
      }
      if (
        typeof obj[currKey] === "undefined" ||
        typeof obj[currKey] !== "object"
      ) {
        obj[currKey] = {};
      }
      return setNestedObjectValueFromPathArray(
        obj[currKey],
        pathArray.slice(1),
        value
      );
    }
    return setNestedObjectValueFromPathArray(obj, pathString.split("."), value);
  }
  
  const arraysToBeFlattened= %s 
  
  function handleRowJS(row) {
    for (let arrPath of Object.keys(arraysToBeFlattened)) {
      let currObj = {};
      let inputConceptIdList = getNestedObjectValue(row, arrPath);
      if (!inputConceptIdList || inputConceptIdList.length === 0) continue;
      inputConceptIdList = inputConceptIdList.map(v => +v);
      for (let cid of arraysToBeFlattened[arrPath]) {
        if (inputConceptIdList.indexOf(cid) >= 0) {
          currObj["%s" + cid] = 1;
        } else currObj["%s" + cid] = 0;
      }
      setNestedObjectValue(row, arrPath, currObj);
    }
    return JSON.stringify(row);
  }
  const row = JSON.parse(input_row);
  return handleRowJS(row);
""";
----- End of user-defined JavaScript functions -----


----- Beginning of query body -----
CREATE OR REPLACE TABLE
  `%s` -- destination_table
  OPTIONS (description=\"%s\") -- table_description
  AS (
  WITH
    json_data AS (
    SELECT
      Connect_ID,
      [handleRow(TO_JSON_STRING(input_row))] AS body
    FROM
      \`%s\` AS input_row -- source_table
    %s), -- filter_statement
    flattened_data AS (
    SELECT
      %s -- selects
    FROM
      json_data,
      UNNEST(body) AS ROW )
  SELECT
    *
  FROM
    flattened_data 
  %s -- order statement
  );
    
'
  
  # Use sprint to put variables in place of "%s" in sql_body
  query <- sprintf(sql_body,
                   notes,
                   arrays_to_be_flattened,
                   var_prefix,
                   var_prefix,
                   destination_table,
                   table_description,
                   source_table,
                   filter_statement,
                   selects,
                   order_statement )
  
  if (length(output_file_path) != 0) {
    fileConn <- file(output_file_path, open="w+")
    writeLines(query, fileConn)
    close(fileConn)
  }
  
  return(query)
  
}

### HELPER FUNCTIONS ###

#' Helper function for generate_flattening_query function. Converts JSON file 
#' with list of variables--that have JSON arrays as values--to a list of strings
#' with the full paths. 
#'
#' @param json JSON file with list of variables--that have JSON arrays as values
#' @param var_prefix A string indicating the prefix used to seperate variable names, i.e., "D_"
#' @return character arary with the full paths, i.e., D_parent.D_child
#' @export
#'
#' @examples
#' # Make a test json file in the form that we use for flattening Connect Modules.
#' json_str <- '{\n\t"D_149884127": [152773041, 249341444], \n\t"D_152138929": [191656389, 243596698, 283652434]\n}'
#' write(json_str, "test.json")
#' 
#' # Make sure you can read it back in and it looks as expected
#' json_str <- paste(readLines("test.json"), collapse='\n')
#' cat(json_str) 
#' 
#' full_paths <- generate_paths_from_cid_list("test.json", var_prefix="D_")
#' 
#' typeof(full_paths)
#' [1] "character"
#' 
#' print(full_paths, width=80)
#' [1] "D_149884127.D_152773041" "D_149884127.D_249341444"
#' [3] "D_152138929.D_191656389" "D_152138929.D_243596698"
#' [5] "D_152138929.D_283652434"
  
generate_paths_from_cid_list <- function(json, var_prefix='D_') {
  
  parent_variables <- jsonlite::read_json(json) # Load JSON as list
  var_prefix  <- paste0('.', var_prefix)
  ## Generate a list of full paths with separation between vars like ".D_" or ".d_"
  full_paths <- c()
  for (parent_name in names(parent_variables)) {
    for (child_name in parent_variables[parent_name]) {
      full_paths <- append(full_paths,
                           paste(parent_name, child_name, sep=var_prefix))
    }
  }
  return(full_paths)
}


#' Helper function. Generate "select" rows to insert into body of SQL query. 
#'
#' @param variables A character array of variable names, i.e., CIDS
#'
#' @return 
#' @export
#'
#' @examples
#' vars <- c("D_149884127.D_152773041", "D_149884127.D_249341444",
#'           "D_152138929.D_191656389", "D_152138929.D_243596698", 
#'           "D_152138929.D_283652434")
#' selects <- generate_selects(vars)
#' 
#' typeof(selects)
#' [1] "character"
#' 
#' cat(selects)
#' # 	REPLACE(JSON_QUERY(row,'$.D_149884127.D_152773041'), '\"', '') AS D_149884127_D_152773041,
#' #  REPLACE(JSON_QUERY(row,'$.D_149884127.D_249341444'), '\"', '') AS D_149884127_D_249341444,
#' #  REPLACE(JSON_QUERY(row,'$.D_152138929.D_191656389'), '\"', '') AS D_152138929_D_191656389,
#' #  REPLACE(JSON_QUERY(row,'$.D_152138929.D_243596698'), '\"', '') AS D_152138929_D_243596698,
#' #  REPLACE(JSON_QUERY(row,'$.D_152138929.D_283652434'), '\"', '') AS D_152138929_D_283652434
#' 
generate_selects <- function(variables = '') {
  
  if (length(variables) == 0) {
    
    # If the list is empty, return a simple line of SQL code: 
    return('* except(Connect_ID)')
    
  } else {
    
    # Otherwise modify the string in each row 
    # variables_ <- variables %>% lapply(function(x) gsub('\\.', '_', x)) %>% simplify()
    str_format <- "\tREPLACE(JSON_QUERY(row,'$.%s'), '\\\"', '') AS %s"
    f          <- function(x) gsub('\\.', '_', x)
    selects    <- variables %>% 
                  lapply(function(x) sprintf(str_format, x, f(x))) %>% 
                  purrr::simplify()
    selects    <- paste(selects, collapse=',\n')
    
    return(selects)
  }
  
}



# # Code for testing:
# destination_table <- 'FlatConnect.module2_v2_JP'
# table_description <- 'this table is just a test'
# source_table      <- 'nih-nci-dceg-connect-dev.Connect.module2_v2'
# json_file         <- 'module2_v2-lists.json'
# csv_file          <- 'module2_v2-variables.csv'
# filter_statement  <- 'WHERE Connect_ID IS NOT NULL'
# output_file_path  <- 'test.sql'
# var_prefix        <- 'D_'
# 
# query <- generate_flattening_query(destination_table = destination_table,
#                                    table_description = table_description,
#                                    source_table      = source_table,
#                                    variable_csv      = csv_file,
#                                    arrays_json       = json_file,
#                                    filter_statement  = filter_statement,
#                                    output_file_path  = output_file_path,
#                                    var_prefix        = var_prefix)
# cat(query)