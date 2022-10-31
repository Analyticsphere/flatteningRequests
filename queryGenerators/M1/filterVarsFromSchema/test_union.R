library(jsonlite)
library(tools)

# A = "json_A.json"
# B = "json_B.json"
# C = "json_C.json"

A = "M1V1-lists-auto.json"
B = "M1-lists.json"
C = "M1-lists-union.json"

# generate_query_files_from_schema()

source("connect_analytics_helper_functions.R")
union_json_str <- get_union_of_json_arrays(A, B, C)
union_json_str

# Get union of variable lists
A_csv <- "M1V1-variables-auto.csv"
B_csv <- "M1-variables.csv"
C_csv <- "M1V1-variables-union.csv"
union_var_chr_array <- get_union_of_variable_csv(A_csv, B_csv, C_csv)

# Deal with exceptions
json_file         <- "M1-lists-union.json"
variables_file    <- "M1V1-variables-auto.csv"

# Get a list of variables with arrays containing "exception CID"
exceptions        <- c(178420302, 958239616, 353358909, 104430631)
vars_w_exceptions <- get_vars_with_specified_vals(json_file, 
                                                  exceptions, 
                                                  max_array_len = 2) 
vars_w_exceptions

# Remove list of variables with exceptions from the data set
updated_variables <- remove_vars_from_json(json_file, 
                                           vars_w_exceptions,
                                           write_to_file = TRUE, 
                                           output_filename = 
                                             "M1V1-lists-union-mod.json") 
var_list

# Add list of variables with exceptions to *-variables.csv
add_novel_vars_to_csv(vars_w_exceptions, variables_file, 
                      out_file = "M1V1-variables-auto-new.csv")


