#' JSON schema to CSV
#'
#' Convert GCP table schema from JSON file to CSV file.
#' 
#' Written by: Jake Peters
#' Date: March 20, 2023
#'
#' @param json_file_name  Name of the GCP schema as JSON file.
#' @param csv_file_name   Name of the CSV file to schema to.
#' @param names_to_filter Array variable names to filter out.
#' @param types_to_filter
#'
#' @return A tibble/dataframe of the schema with 'name', 'type', and 'mode'.
#' @export
#'
#' @examples
#' # Filter some names and types from csv and export to csv. Return a dataframe.
#' names_to_filter <- c("__key__", "__error__", "__has_error__",
#'                      "treeJSON", "allEmails", "allPhoneNo")
#' types_to_filter <- c("RECORD")
#' json_schema_to_csv("schema.json", "schema.csv",
#'                    names_to_filter, types_to_filter)

json_schema_to_csv <- function(json_file_name,
                               csv_file_name,
                               names_to_filter,
                               types_to_filter) {
  # Make sure required packages are installed
  list.of.packages <- c("jsonlite", "tibble", "dplyr")
  new.packages <-
    list.of.packages[!(list.of.packages %in% installed.packages()[, "Package"])]
  if (length(new.packages))
    install.packages(new.packages)
  
  # Convert JSON to list and initialize queue
  data_queue <- jsonlite::read_json(json_file_name)
  
  # Initialize output
  data_out <- tibble(name = character(),
                     mode = character(),
                     type = character())
  
  # Loop through queue while it contains objects
  while (length(data_queue) != 0) {
    # Remove first object from queue
    data_obj      <- data_queue[1]
    data_queue[1] <- NULL
    
    if ('fields' %in% names(data_obj[[1]])) {
      # Has children. Add each child to queue w/ full path parent.child name.
      parent_name <- data_obj[[1]]$name
      for (child_obj in data_obj[[1]][['fields']]) {
        child_obj$name <- paste(parent_name, child_obj$name, sep = ".")
        data_queue <- append(data_queue, list(child_obj))
      }
      
    } else {
      # Does not have children, dump data to file
      data_out <- rbind(
        data_out,
        tibble(
          name = data_obj[[1]]$name,
          mode = data_obj[[1]]$mode,
          type = data_obj[[1]]$type
        )
      )
    }
    
  }
  
  # Filter out variables with unwanted names or types and write to csv
  data_out <- data_out %>%
    filter(!grepl(paste(names_to_filter, collapse = "|"), name)) %>%
    filter(!grepl(paste(types_to_filter, collapse = "|"), type)) 
  
  write.csv(data_out, csv_file_name, row.names = FALSE)
  
  return(data_out)
}