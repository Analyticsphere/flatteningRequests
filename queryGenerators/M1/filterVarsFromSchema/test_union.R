library(jsonlite)
library(tools)

A = "json_A.json"
B = "json_B.json"
C = "json_C.json"

source("get_union_of_json_arrays.R")
union_json_str <- get_union_of_json_arrays(A, B, C)
union_json_str