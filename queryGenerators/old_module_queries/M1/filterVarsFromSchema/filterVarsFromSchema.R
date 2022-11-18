# File:       getVarsFromSchema.R
# Decription: Read Module 1 Schema from excel file. Filter variables and export 
#             to M1-variables.csv and M1-lists.js files.
# Author:     Jake Peters
# Date:       October 2022

library("readxl")
library("dplyr")
library("bigrquery")

# Change project and billing info as needed.
project <- "nih-nci-dceg-connect-prod-6d04"  
billing <- project # Billing should be same as project
table   <- "Connect.participants"
bq_auth() # Authenticate with BigQuery (Note: project & billing used here)

## Get schema data for Version 1 and Version 2
excel_file <- "/Users/petersjm/Documents/dataFlattening/autoFlattening/module_1_schemas.xlsx"
df_V1 <- read_excel(excel_file, sheet = "M1V1") # Module 1 Version 1 Schema
df_V2 <- read_excel(excel_file, sheet = "M1V2") # Module 1 Version 2 Schema

# Make all columns factors so they can be used when filtering
df_V1[] <- lapply(df_V1, factor) # the "[]" keeps the dataframe structure
df_V2[] <- lapply(df_V2, factor) 

## Filter out RECORDS (variables with nested data) and keys
df_V1_filt <- df_V1 %>% filter(!grepl("__key__", fullname)) %>% filter(!grepl("RECORD", type_))
df_V2_filt <- df_V2 %>% filter(!grepl("__key__", fullname)) %>% filter(!grepl("RECORD", type_))

## Get list of variables that do not repeat (destined for M2*-variables.csv)
df_V1_vars <- df_V1_filt %>% filter(!grepl("REPEATED", mode))
df_V2_vars <- df_V2_filt %>% filter(!grepl("REPEATED", mode))

## Get variables that do repeated (destined for M2*-lists.js)
df_V1_lists <- df_V1_filt %>% filter(grepl("REPEATED", mode))
df_V2_lists <- df_V2_filt %>% filter(grepl("REPEATED", mode)) 

## Export variables to M2V*-variables.csv file for queryGenerator
write.table(df_V1_vars$fullname, file="M1V1-variables_auto.csv", 
            col.names=FALSE, row.names=FALSE, quote=FALSE, sep="," )
write.table(df_V2_vars$fullname, file="M1V2-variables_auto.csv", 
            col.names=FALSE, row.names=FALSE, quote=FALSE, sep="," )

## Generate M2V*-lists.js file for queryGenerator.js

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
  
  # TODO Remove query from loop and do whole set at once
  
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

# TODO Use JSON lite to generate json file from R list rather than using string
# concatenation to contstruct the json file

# Define strings to open and close M2V*-lists.js files
opening_line <- "const pathToConceptIdList = {"
closing_line <- "};"
ending_line  <- "module.exports=pathToConceptIdList;"

# Write lines for V1
fidM1V1 = "M1V1-lists-auto.js" # destination file
write(opening_line, file=fidM1V1, append=FALSE) # Overwrite existing file
cnt <- 0
for (var_name in df_V1_lists$fullname) {
      cnt            <- cnt + 1
      responses_list <- get_list_of_unique_responses(var_name, project, table)
      responses_str  <- paste(responses_list, collapse=", ")
      var_line       <- paste0("'", var_name, "': [", responses_str, "],")
      print(paste(cnt, "of", length(df_V1_lists$fullname), sep=" "))
      print(var_line)
      write(var_line, file=fidM1V1, append=TRUE) # Do not overwrite, just append
}
write(closing_line, file=fidM1V1, append=TRUE)
write(ending_line,  file=fidM1V1, append=TRUE)

# Write lines for V2
fidM1V2 = "M1V2-lists-auto.js" # destination file
write(opening_line, file=fidM1V2, append=FALSE)
cnt <- 0
for (var_name in df_V2_lists$fullname) {
      cnt            <- cnt + 1
      responses_list <- get_list_of_unique_responses(var_name, project, table)
      responses_str  <- paste(responses_list, collapse=", ")
      var_line       <- paste0("'", var_name, "': [", responses_str, "],")
      print(paste(cnt, "of", length(df_V2_lists$fullname), sep=" "))
      print(var_line)
      write(var_line, file=fidM1V2, append=TRUE)
}
write(closing_line, file=fidM1V2, append=TRUE)
write(ending_line,  file=fidM1V2, append=TRUE)


