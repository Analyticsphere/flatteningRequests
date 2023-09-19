#' Get BigQuery Table Schema and Dictionary Data Information
#'
#' This function retrieves schema information from BigQuery and variable 
#' metadata from the Connect for Cancer Data Dictionary..
#' 
#' Written: September 2023
#' Last Updated: September 2023
#'#' @author Jake Peters
#' 
#' @param project The BigQuery project ID.
#' @param dataset The BigQuery dataset name.
#' @param table The BigQuery table name.
#' @return A data frame containing schema information and variable metadata.
#'
#' @importFrom bigrquery bq_auth bq_project_query bq_table_download
#' @importFrom glue glue
#' @importFrom stringr str_extract_all
#'
#' @examples
#' project <- "nih-nci-dceg-connect-prod-6d04"
#' dataset <- "FlatConnect"
#' table   <- "participants_JP"
#' sch_dict_df <- get_bq_schema_and_dictionary(project, dataset, table)
#'
get_bq_schema_and_dictionary <- function(project, dataset, table) {
  # Load required libraries
  library(bigrquery)
  library(glue)
  library(stringr)
  
  # Get master data dictionary
  urlfile <- "https://raw.githubusercontent.com/episphere/conceptGithubActions/master/csv/masterFile.csv" 
  data_dict <- read.csv(urlfile)
  
  # Authenticate to BigQuery
  billing <- project # Project and billing should be consistent
  bq_auth()
  
  # Get schema information from BigQuery
  query <- glue(
    "SELECT * ",
    "FROM  `{project}.{dataset}`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS ",
    "WHERE table_name='{table}'"
  )
  tbl    <- bq_project_query(project, query = query)
  sch_df <- bq_table_download(tbl, bigint = "integer64")
  
  # Define a function to extract 9-digit numbers from a string
  extract_numbers <- function(text) {
    numbers <- str_extract_all(text, "\\d{9}")
    if (length(numbers[[1]]) > 0) {
      return(paste(numbers[[1]], collapse = ", "))
    } else {
      return(NA)
    }
  }
  
  # Apply the custom function to each element of 'vars' and store the result as strings
  sch_df$cids <- sapply(sch_df$column_name, extract_numbers)
  
  # Add a new column 'last_cid' containing the last CID from the 'cids' column
  sch_df$last_cid <- sapply(strsplit(as.character(sch_df$cids), ", "), function(x)
    tail(x, 1))
  
  # Get metadata for variables from data dictionary
  idx <- which(data_dict$conceptId.3 != "")
  var_metadata <- data.frame(
    last_cid = data_dict$conceptId.3[idx],
    pii = ifelse(tolower(trimws(data_dict$PII[idx])) == 'yes', TRUE, FALSE),
    variable_name  = data_dict$Variable.Name[idx],
    variable_label = data_dict$Variable.Label[idx],
    question_name = data_dict$Current.Source.Question[idx],
    question_text = data_dict$Current.Question.Text[idx],
    primary_source = data_dict$Primary.Source[idx],
    secondary_source = data_dict$Secondary.Source[idx]
  )
  
  # Merge the data frames based on the 'cid' column
  output_df <- merge(sch_df, var_metadata, by = c("last_cid"), all.x = TRUE)
  
  # Create a 'description' column by combining 'variable_name' and 'variable_label'
  output_df$description <- paste(output_df$variable_name, output_df$variable_label, sep = ' -- ')
  
  # Reorder the dataframe
  new_order <- c("table_catalog","table_schema","table_name",
                 "column_name","field_path","data_type","description",
                 "collation_name","rounding_mode","cids","last_cid","pii",
                 "variable_name","variable_label",
                 "question_name","question_text",
                 "primary_source","secondary_source")
  output_df <- output_df[new_order]
  
  return(output_df)
}