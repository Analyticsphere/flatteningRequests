get_union_of_json_arrays <- function(json_A, json_B, json_C_filename) {
    require(jsonlite)
    require(tools)
  
    # If a and b are files 
    if (tools::file_ext(A) == "json") {
      list_A <- jsonlite::fromJSON(json_A)
      list_B <- jsonlite::fromJSON(json_B)
    }
    list_C <- list()
    
    # Get variable names unique to A, unique to B or common to both
    var_names_uniq_to_A   <- setdiff(names(list_A), names(list_B))
    var_names_uniq_to_B   <- setdiff(names(list_B), names(list_A))
    var_names_in_A_and_B  <- intersect(names(list_A), names(list_B))
    
    # Add variables/array values common to both A and B to C
    for (variable_name in var_names_in_A_and_B) {
      # Get arrays assigned to list_A$variable_name and list_B$variable_name
      array_A <- eval(parse(text = paste0("list_A$", variable_name)))
      array_B <- eval(parse(text = paste0("list_B$", variable_name)))
      
      # Get array of unique values found in A and B combined
      array_C <- union(array_A, array_B)
      
      # Add array to list_C under listC$variable_name
      eval(parse(text = (
        paste0("list_C$", variable_name, " <- array_C")
      )))
    }
    
    # Add variables unique to list A to list C
    for (variable_name in var_names_uniq_to_A) {
      array <- eval(parse(text = paste0("list_A$", variable_name)))
      eval(parse(text = (
        paste0("list_C$", variable_name, " <- array")
      )))
    }
    
    # Add variables unique to list B to list C
    for (variable_name in var_names_uniq_to_B) {
      array <- eval(parse(text = paste0("list_B$", variable_name)))
      eval(parse(text = (
        paste0("list_C$", variable_name, " <- array")
      )))
    }
    
    # Sort variables in list_C
    list_C <- lapply(list_C, sort)         # Sort CIDs within JSON arrays
    list_C <- list_C[order(names(list_C))] # Sort variables
    
    # Convert list C to JSON string
    list_C_as_JSON_string <- jsonlite::prettify(jsonlite::toJSON(list_C),
                                                indent = 4)
    # Export JSON string to file
    write(list_C_as_JSON_string, json_C_filename)
    
    return(list_C_as_JSON_string)
    
  }