library(glue)
library(bigrquery)
#source("https://raw.githubusercontent.com/Analyticsphere/flatteningRequests/main/gcp_query_tools/connect_analytics_helper_functions.R")

get_gcp_table_schema <- function(project, table) {
  bash_cmd_str <- 
    paste0("bq show --schema --format=prettyjson ", project, ":", table)
  schema <- system(bash_cmd_str, intern=TRUE)
}

get_cid_vector <- function(compound_cid_name){
  # get vector of string fragments that are numbers, i.e, no "D_" or "_d_" etc.
  cid_vector <- strsplit(compound_cid_name,'[^0-9]') %>%
    lapply(function(x){x[!x ==""]}) %>% # remove empty strings
    unlist()                            # convert from list to character
  cid_vector
}

dictionary_lookup <- function(x, dict){
  x <- x %>% 
    str_split_i("d_", -1) %>% # Get last CID without "d_"
    map(~dict[[.x]]) %>% 
    modify_if(is.null,~"NA")
  x
}

get_cid_description <- function(column_name,
                                dictionary_json_url="https://episphere.github.io/conceptGithubActions/aggregate.json"){
  
  cid_vector <- get_cid_vector(column_name)
  # Get dictionary
  dict <- rio::import(dictionary_json_url,format = "json") %>% 
    map(~.x[["Variable Label"]] %||% .x[["Variable Name"]]) %>% 
    compact()
  
  # Get descriptions for each CID and put in a vector
  description_vector <- rep("NA", length(cid_vector))
  for (i in 1:length(cid_vector)){
    description_vector[i] <- dictionary_lookup(cid_vector[i], dict)
  }
  
  # Convert vector of descriptions to one compound description with "; " delim.
  description <- paste(description_vector, collapse = " -> ")
}

project  <- "nih-nci-dceg-connect-prod-6d04"
dataset  <- "FlatConnect"
table    <- "participants_JP"
dict_url <- "https://raw.githubusercontent.com/episphere/conceptGithubActions/master/csv/masterFile.csv"
biilling <- project


  
dictionary <- read.csv(dict_url)
bq_auth()
sql <- glue("SELECT * ",
            "FROM  `{project}.{dataset}`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS ",
            "WHERE table_name='{table}'")
query     <- bq_project_query(project, sql)
var_tab   <- bigrquery::bq_table_download(query, bigint = "integer64")

# Function to tell whether column_name, x, has a cid in it or not
has_cid <- function(x) {!is_empty(get_cid_vector(x)[[1]])}

schema <- var_tab %>%
  mutate(description = map_chr(column_name, get_cid_description)) %>%
  select(name = column_name,
         type = data_type,
         description = description)

schema_json <- jsonlite::prettify(jsonlite::toJSON(schema))

                                  
  # explanation = case_when(
  #   qc_test == "valid" ~
  #     (function(x) 
  #       glue("[{x$ConceptID_lookup}] should be [{x$ValidValues_lookup}], but it's [{x$invalid_values_lookup}].")
  #     ) (x),



# project  <- "nih-nci-dceg-connect-prod-6d04"
# dataset  <- "FlatConnect"
# table    <- "participants_JP"
# dict_url <- "https://raw.githubusercontent.com/episphere/conceptGithubActions/master/csv/masterFile.csv" 
# 
# all_vars <- names(var_tab)
# pii_vars <- filter_PII_vars(project, dataset, table)
# no_pii_vars <- names(var_tab[,!(names(var_tab) %in% pii_vars)])