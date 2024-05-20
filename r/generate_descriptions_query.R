generate_descriptions_query <- function(full_table_name, 
                                        variables = '', 
                                        metadata_df) {
  
  library(purrr)
  library(dplyr)
  library(glue)
  
  if (length(variables) == 0) {
    
    # If the list is empty, return an empty string: 
    return('')
    
  } else {
    
    # Otherwise modify the string in each row 
    header_str   <- glue::glue("ALTER TABLE `{full_table_name}`")
    f            <- function(x) gsub('\\.', '_', x)
    describe     <- function(x) {metadata_df[metadata_df$column_name==x,range(1,10)]$description}
      #metadata_df[metadata_df['column_name']==x]['description']}
    descriptions <- variables %>% 
      lapply(function(x) glue::glue("\tALTER COLUMN {f(x)} SET OPTIONS (description='{describe(f(x))}')")) %>%
      paste(collapse=",\n")

    return(descriptions)
  }
  
}


# ALTER VIEW `nih-nci-dceg-connect-dev.BQ2.stakeholderMetrics`
# ALTER COLUMN d_100767870 SET OPTIONS (description="SMMet_BaseSrvCompl_v1r0, All baseline surveys completed"),

source('gcp_query_tools/get_bq_schema_and_dictionary.R')

variables     <- c('D_149884127.D_152773041')
variables     <- metadata_df$column_name
table         <- 'nih-nci-dceg-connect-dev.FlatConnect.module2_v2_JP'
table_details <- str_split(table, pattern='\\.')[[1]]
project_id    <- table_details[1]
dataset_id    <- table_details[2]
table_id      <- table_details[3]
metadata_df   <- get_bq_schema_and_dictionary(project_id, dataset_id, table_id)
descriptions  <- generate_descriptions_query(table, variables, metadata_df)
