#' Update SQL query in a scheduled transfer configuration in BigQuery.
#' 
#' This function updates the SQL query in a scheduled transfer configuration in BigQuery.
#' 
#' @param project_id The project ID in Google Cloud Platform.
#' @param sql_query The SQL query to be updated in the scheduled transfer configuration.
#' @param transfer_config_id The ID of the transfer configuration to be updated.
#' @param scopes A character vector specifying the OAuth scopes required for authentication.
#' 
#' @return A message indicating whether the scheduled query was updated successfully or not.
#' 
#' @import gargle httr RJSONIO
#' @export
#' 
#' @author Jake Peters
#' @date 2024-03-14
#' 
#' @examples
#' 
#' project_id <- "your-project-dev"
#' sql_query  <- "SELECT 5;"
#' transfer_config_id <- "65fb7298-0000-285d-816b-14223bb25152"
#' 
#' update_scheduled_query(project_id, sql_query, transfer_config_id)
#' 
update_scheduled_query <- function(project_id, sql_query, transfer_config_id, 
                                   scopes = c("https://www.googleapis.com/auth/bigquery",
                                              "https://www.googleapis.com/auth/cloud-platform")) {
  
  # Install packages if not already installed
  if (!requireNamespace("gargle", quietly = TRUE)) {
    install.packages("gargle")
  }
  if (!requireNamespace("httr", quietly = TRUE)) {
    install.packages("httr")
  }
  if (!requireNamespace("RJSONIO", quietly = TRUE)) {
    install.packages("RJSONIO")
  }
  
  library(gargle)
  library(httr)
  library(RJSONIO)
  
  # Retrieve OAuth token
  token_info  <- gargle::token_fetch(scopes = scopes)
  oauth_token <- token_info$credentials$access_token
  
  url <- paste0("https://bigquerydatatransfer.googleapis.com/v1/projects/",
                project_id,
                "/transferConfigs/",
                transfer_config_id)
  
  # Make GET request with OAuth token in headers
  response <- httr::GET(url, httr::add_headers(Authorization = paste("Bearer", oauth_token)))
  
  if (response$status_code == 200) {
    transfer_config <- httr::content(response)
    # Assuming the transfer configuration is in JSON format and has a field named 'params'
    # You might need to adapt this based on the actual structure of your transfer configuration
    transfer_config$params$query = sql_query
    
    # Patch the transfer configuration with the updated SQL query
    patch_response <- httr::PATCH(url, 
                                  body = RJSONIO::toJSON(transfer_config), 
                                  add_headers("Content-Type" = "application/json",
                                              Authorization = paste("Bearer", oauth_token)),
                                  encode = "json",
                                  query = list(update_mask = "params"))  # Specify the update_mask field
    
    if (patch_response$status_code == 200) {
      return("Scheduled query updated successfully.")
    } else {
      warning("Failed to update scheduled query.")
      print(patch_response)
    }
  } else {
    warning("Failed to retrieve transfer configuration details.")
    print(response)
  }
}
