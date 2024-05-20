# Before using this code, you must install BQ CLI: 
#       https://cloud.google.com/sdk/docs/install
# 
# This function checks the status of a scheduled query using the BQ CLI and 
# returns a status of "SUCCEEDED", "PENDING", "RUNNING" or "FAILED".
# 
# You could use it at the start of your Connect report scripts to check if the 
# most recent scheduled query needed for your report ran successfully. If it did
# not, you can reach out to Jake Peters or investigate in BigQuery. 
#
# Example Usage:
#
# table_name <- "FlatConnect.participants_JP"
# project    <- "nih-nci-dceg-connect-prod-6d04"
# print(check_sch_query_status(project, table_name))
# # Check status of scheduled query
# query_status <- check_sch_query_status(project, table_name)
# 
# switch(query_status,
#        "FAILED"    = stop(paste0("The sch. query for ", table_name, " failed. Reach out to Jake.")),
#        "PENDING"   = stop(paste0("The query for ", table_name, " is scheduled and waiting to be run.")),
#        "RUNNING"   = stop(paste0("The sch. query for ", table_name, " is still running. Wait until complete.")),
#        "SUCCEEDED" = print(paste0("The most. recent scheduled query for ", table_name, " succeeded. The table is up-to-date."))
# )

check_sch_query_status <- 
  function(project, table_name){

    # Set default project for BQ CLI
    # set_proj_cmd <- paste0("bq ls -j --project_id ", project, " > /dev/null")
    # system(set_proj_cmd)
    
    # Get list of scheduled queries from bq
    bq_request <- paste0('bq ls --transfer_config --transfer_location="us" --filter="dataSourceIds:scheduled_query" --project_id=',project)
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
    
    # Return sch. query status as "SUCCEEDED", "PENDING", "RUNNING" or "FAILED".
    df$state[df$displayName==table_name] 
  }