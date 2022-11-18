library(jsonlite)
library(tools)

project  <- "nih-nci-dceg-connect-prod-6d04"
table    <- "Connect.participants"
schema   <- "/Users/petersjm/Documents/dataFlattening/autoFlattening/module_1_schemas.xlsx"
out_json <- "M1V1-lists-auto.js" # destination file
out_csv  <- "M1V1-variables_auto.csv"
sheet    <- "M1V1"

filter_vars_from_schema(project, table, schema, out_json, out_csv, sheet=NULL)

# A <- "json_A.json"
# B <- "json_B.json"
# C <- "json_C.json"

A <- "M1V1-lists-auto.json"
B <- "M1-lists.json"
C <- "M1-lists-union.json"
# A <- "M1V2-lists-auto.json"
# B <- "M1V2-lists.json"
# C <- "M1V2-lists-union.json"

source("connect_analytics_helper_functions.R")
union_json_str <- get_union_of_json_arrays(A, B, C)
union_json_str

# Get union of variable lists
# Get union of variable lists
# A_csv <- "M1V2-variables-auto.csv"
# B_csv <- "M1V2-variables.csv"
# C_csv <- "M1V2-variables-union.csv"
A_csv <- "M1V1-variables-auto.csv"
B_csv <- "M1-variables.csv"
C_csv <- "M1V1-variables-union.csv"
union_var_chr_array <- get_union_of_variable_csv(A_csv, B_csv, C_csv)

# Deal with exceptions
json_file         <- "M1-lists-union.json"
variables_file    <- "M1V1-variables-auto.csv"
# json_file         <- "M1V2-lists-union.json"
# variables_file    <- "M1V2-variables-auto.csv"

# Get a list of variables with arrays containing "exception CID"
exceptions        <- c(178420302, 958239616, 353358909, 104430631)
vars_w_exceptions <- get_vars_with_specified_vals(json_file, 
                                                  exceptions, 
                                                  max_array_len = 2) 
vars_w_exceptions

# Remove list of variables with exceptions from the data set
output_filename  <- "M1V1-lists-union-mod.json"
# output_filename   <- "M1V2-lists-union-mod.json"
updated_variables <- remove_vars_from_json(json_file, 
                                           vars_w_exceptions,
                                           write_to_file = TRUE, 
                                           output_filename = output_filename) 
updated_variables

# Add list of variables with exceptions to *-variables.csv
out_file <-"M1V1-variables-auto-new.csv"
# out_file <-"M1V2-variables-auto-new.csv"
add_novel_vars_to_csv(vars_w_exceptions, variables_file, 
                      out_file = out_file)


