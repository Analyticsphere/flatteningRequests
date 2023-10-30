get_schema_using_cli <- function(project, dataset_id, table_id) {
  bash_cmd_str   <- paste0("bq show --schema --format=prettyjson ", 
                           project, ":", dataset_id, ".", table_id)
  schema         <- system(bash_cmd_str, stdout = TRUE)
  schema         <- jsonlite::fromJSON(schema)
  schema         <- jsonlite::toJSON(schema, pretty = TRUE, auto_unbox = TRUE)
  return(schema)
}

get_schema_using_bigrquery <- function(){
  bq_table       <- bq_table(project, dataset_id, table_id)
  metadata       <- bigrquery::bq_table_meta(bq_table)
  schema         <- metadata$schema$fields
  schema_as_json <- jsonlite::toJSON(schema, pretty = TRUE, auto_unbox = TRUE)
  return(schema)
}

project          <- ""
dataset_id       <- ""
table_id         <- ""

schema_cli       <- get_schema_using_cli(project, dataset_id, table_id) 
schema_bigrquery <- get_schema_using_bigrquery(project, dataset_id, table_id)
is_equal         <- identical(schema_cli, schema_bigrquery)
is_equal
