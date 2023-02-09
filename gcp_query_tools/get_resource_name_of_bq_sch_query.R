# Before using this code, you must install BQ CLI: 
#       https://cloud.google.com/sdk/docs/install
# 
# This function uses the bq CLI to look up the resource_name of a scheculed 
# query. This resource name is useful for scheduling a query using the API or 
# the BQ CLI.
#
# Example Usage:
# query_name    <- "FlatConnect.participants_JP"
# project       <- "nih-nci-dceg-connect-prod-6d04"
# resource_name <- get_resource_name_of_bq_sch_query(project, query_name)
# print(resource_name)
# # [1] projects/155089172944/locations/us/transferConfigs/63a134bd-0000-2c48-8c16-883d24f91d30"

get_resource_name_of_bq_sch_query <- 
  function(project_id, query_name){
    
    # Get list of scheduled queries from bq
    bq_request <- paste0("bq ls --transfer_config --transfer_location='us' --project_id ", project_id, " --filter='dataSourceIds:scheduled_query'")
    response   <- system(bq_request, intern=TRUE)
    # Remove second line "----" of table and remove white space from each row
    response   <- lapply(response[-2], function(x) scan(text = x, what = ""))
    header     <- simplify2array(response[1]) # Array of column names
    
    # Bind rows of data into one array
    for (i in length(header)){
      x <- c() # Initialize array with first non-header line of data
      # Loop through remaining data
      for (line in response[2:length(response)]){
        x <- rbind(x, line)
      }
    }
    
    # Format as dataframe
    df <- data.frame(x, row.names = NULL)
    colnames(df) <- header
    
    # Return resource name
    # ex: projects/155089172944/locations/us/transferConfigs/64120cbd-0000-2223-9479-089e08289458
    if (length(df$name[df$displayName==table_name]) == 1) {
      return(df$name[df$displayName==table_name])
    } else if (length(df$name[df$displayName==table_name]) == 0) {
      return(NULL)
    } else if (length(df$name[df$displayName==table_name]) > 1) {
      stop(paste0("This query_name is not unique!"))
    }
    
  }
