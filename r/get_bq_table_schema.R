#' Get the schema of a BigQuery table and optionally save it as JSON
#'
#' This function retrieves the schema of a BigQuery table in a specified Google Cloud project, dataset, and table.
#'
#' @param project_id A string representing the Google Cloud project ID.
#' @param dataset_id A string representing the BigQuery dataset ID.
#' @param table_id A string representing the BigQuery table ID.
#' @param as_json A logical value indicating whether to return the schema as JSON (default) or as a list.
#'
#' @return The schema of the specified BigQuery table as either a JSON string or a list, depending on the 'as_json' parameter.
#'
#' @examples
#'
#' project_id <- "your-project-id"
#' dataset_id <- "your-dataset-id"
#' table_id   <- "your-table-id"
#'
#' # Retrieve schema as JSON and save it to a file
#' schema_json <- get_bq_table_schema(project_id, dataset_id, table_id)
#' writeLines(schema_json, "schema.json")
#'
#' # Retrieve schema as a list
#' schema_list <- get_bq_table_schema(project_id, dataset_id, table_id, as_json = FALSE)
#' write(schema, file = 'schema.json')
#'
#' @import jsonlite
#' @import bigrquery
get_bq_table_schema <- function(project_id, dataset_id, table_id, as_json = TRUE) {

  # Construct BQ table object
  bq_table <- bq_table(project_id, dataset_id, table_id) 
  
  # Retrive the metadata for the table (a list containing the schema)
  metadata <- bigrquery::bq_table_meta(bq_table)
  
  # Simplify the structure of the schema so that it matches that returned by `bq` CLI
  schema <- metadata$schema$fields
  
  # Write the schema to a JSON string
  schema_as_json <- jsonlite::toJSON(schema, pretty = TRUE, auto_unbox = TRUE)
  
  # Return as JSON by default, otherwise return as list
  if (as_json == TRUE) {
    return(schema_as_json)
  } else {
    return(schema)
  }
}