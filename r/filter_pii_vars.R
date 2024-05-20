filter_PII_vars <- function(project, dataset, table,
                         dict_url="https://github.com/episphere/conceptGithubActions/blob/master/csv/masterFile.csv"){
  
  require(glue)
  require(bigrquery)
  require(dplyr)
  
  dictionary <- read.csv(dict_url)
  sql <- glue("SELECT * ",
              "FROM  `{project}.{dataset}`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS ",
              "WHERE table_name='{table}'")
  query     <- bq_project_query(project, sql)
  df        <- bigrquery::bq_table_download(query, bigint = "character")
  
  # Get just the last CID in the compound variable name
  # TODO Add filter to ensure that you get the last number that is the appropriate 
  # length. Modules have d_123456789_v2. We don't want "2". We want "123456789".
  pattern <- "(?<=_)[0-9]{9}(?=_)"
  
  df <- df %>%
    mutate(last.CID = str_extract_all(field_path, pattern) %>% sapply(tail, 1))

  
  # df$last.CID <- ifelse(grepl("D_", df$field_path), 
  #                         substring(sapply(strsplit(df$field_path,"D_"),tail,1),1,9),
  #                         ifelse(grepl("d_", df$field_path), 
  #                                substring(sapply(strsplit(df$field_path,"d_"),tail,1),1,9), 
  #                                NA))
  
  pii_cid <- dictionary$conceptId.3[which(dictionary$PII == "Yes")]
  print(pii_cid)
  # to make the PII=variables in the recruitment table, 
  # which are easily remove in BQ query.
  vars        <- list()
  vars$all    <- df$column_name
  vars$pii    <- df$column_name[which(df$last.CID %in% pii_cid)] 
  vars$no_pii <- vars$all[!(vars$pii %in% vars$all)]
  vars
}

# Uncoment to test filter_PII_vars() function
project  <- "nih-nci-dceg-connect-prod-6d04"
dataset  <- "FlatConnect"
table    <- "participants_JP"
dict_url <- "https://raw.githubusercontent.com/episphere/conceptGithubActions/master/csv/masterFile.csv"
vars <- filter_PII_vars(project, dataset, table)
vars$no_pii
