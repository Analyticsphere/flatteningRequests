library(glue)

vars_to_remove = c("column_name",
"D_122887481.TUBLIG.D_232595513",
"D_122887481.TUBLIG.D_614366597",
"D_173240848.D_210004486",
"D_180961306.D_473901019",
"D_259089008_1_1.SIBCANC3O.D_230633094_1",
"D_259089008_1_1.SIBCANC3O.D_962468280_1",
"D_301414575.DEPRESS2.D_479548517",
"D_301414575.DEPRESS2.D_591959654",
"D_301679110.DM2.D_166195719",
"D_301679110.DM2.D_861769692",
"D_367884741.TONSILS.D_300754548",
"D_367884741.TONSILS.D_714712574",
"D_370198527.DADCANC3K.D_260972338",
"D_370198527.DADCANC3K.D_331562964",
"D_402548942.MOMCANC3D.D_388289687",
"D_402548942.MOMCANC3D.D_734800333",
"D_460062034.BLOODCLOT.D_497018554",
"D_460062034.BLOODCLOT.D_694594047",
"D_530742915.D_204067509",
"D_530742915.D_883468127",
"D_543728565.D_251231642",
"D_550075233.APPEND.D_727704681",
"D_550075233.APPEND.D_919193251",
"D_578895128.1_1.D_747086784",
"D_588212264.D_543494377",
"D_588212264.D_876238155",
"D_624179836.D_668434958",
"D_630675760.D_716502980",
"D_630675760.D_987134866",
"D_700374192.D_745065357",
'D_725626004.D_522161060',
"D_836890480.CHOL.D_470282814",
"D_836890480.CHOL.D_637556277",
'D_846786840.UF.D_351965599',
"D_846786840.UF.D_895115511",
"D_874046190.D_870244556",
"D_874709643.D_572909251",
"D_884793537.HTN.D_367670682",
"D_884793537.HTN.D_608469482",
"D_894259747.D_445446905",
"D_900541533.D_599063442",
"D_907590067_4_4.SIBCANC3D.D_650332509_4",
"D_907590067_4_4.SIBCANC3D.D_932489634_4",
"D_936904328.D_574474707",
"D_936904328.D_694973364",
"D_508846529.D_426285737",
"D_578895128_2_2.D_677702321",
"D_578895128_4_4.D_254709325",
"D_578895128_2_2.D_195093589",
"D_578895128_2_2.D_660424548",
"D_578895128_2_2.D_618107598",
"D_641651634.D_746038746",
"D_812370563_1_1.D_957429734",
"D_812370563_2_2.D_957429734",
"D_812370563_3_3.D_957429734",
"D_812370563_4_4.D_957429734",
"D_814510313.D_618107598",
"D_814510313.D_660424548",
"D_578895128_19_19.D_578895128_19_19",
"D_812370563_8_8.D_812370563_8_8",
"D_857915436.D_284580415")

remove_rows_and_list_removed <- function(data_frame, column, values_to_remove) {
  # Determine if the column is specified by name or index
  if (is.numeric(column)) {
    # It's an index, validate it
    if (column < 1 || column > ncol(data_frame)) {
      stop("Column index out of bounds")
    }
    column_data <- data_frame[[column]]
  } else if (is.character(column)) {
    # It's a name, validate it
    if (!column %in% names(data_frame)) {
      stop("Column name not found in the DataFrame")
    }
    column_data <- data_frame[[column]]
  } else {
    stop("Column parameter must be either a name or an index")
  }
  
  # Filter out rows where the column value is in the values_to_remove list
  filtered_df <- data_frame[!column_data %in% values_to_remove, ]
  
  # Get the rows that have been removed
  removed_rows <- data_frame[column_data %in% values_to_remove, ]
  
  return(list(filtered_df = filtered_df, removed_rows = removed_rows))
}



tier <- "prod"
# tier <- "dev"
# tier <- "stg"
# module <- "module1_v1" 
module <- "module1_v2"
variables_file <- 
  glue(
    "queryGenerators/FlatConnect_queries/{module}/{tier}/{module}-variables.csv"
    )
df <- read.csv(variables_file, header = FALSE)

# Using a column index
result <- remove_rows_and_list_removed(df, 1, vars_to_remove)  # For the first column

# Accessing the filtered DataFrame
filtered_df <- result$filtered_df

# Accessing the removed rows
removed_rows <- result$removed_rows
removed_rows

write.csv2(filtered_df, variables_file, row.names = FALSE, quote = FALSE)

# removed from prod module1_v1
# [1] "D_122887481.TUBLIG.D_232595513"          "D_122887481.TUBLIG.D_614366597"         
# [3] "D_259089008_1_1.SIBCANC3O.D_230633094_1" "D_259089008_1_1.SIBCANC3O.D_962468280_1"
# [5] "D_301414575.DEPRESS2.D_479548517"        "D_301414575.DEPRESS2.D_591959654"       
# [7] "D_301679110.DM2.D_166195719"             "D_301679110.DM2.D_861769692"            
# [9] "D_367884741.TONSILS.D_300754548"         "D_367884741.TONSILS.D_714712574"        
# [11] "D_370198527.DADCANC3K.D_260972338"       "D_370198527.DADCANC3K.D_331562964"      
# [13] "D_402548942.MOMCANC3D.D_388289687"       "D_402548942.MOMCANC3D.D_734800333"      
# [15] "D_460062034.BLOODCLOT.D_497018554"       "D_460062034.BLOODCLOT.D_694594047"      
# [17] "D_550075233.APPEND.D_727704681"          "D_550075233.APPEND.D_919193251"         
# [19] "D_836890480.CHOL.D_470282814"            "D_836890480.CHOL.D_637556277"           
# [21] "D_846786840.UF.D_351965599"              "D_846786840.UF.D_895115511"             
# [23] "D_857915436.D_284580415"                 "D_884793537.HTN.D_367670682"            
# [25] "D_884793537.HTN.D_608469482"             "D_907590067_4_4.SIBCANC3D.D_650332509_4"
# [27] "D_907590067_4_4.SIBCANC3D.D_932489634_4"


# removed from prod module1_v2
# [1] "D_578895128_19_19.D_578895128_19_19" "D_812370563_8_8.D_812370563_8_8"    
# [3] "D_857915436.D_284580415"  
