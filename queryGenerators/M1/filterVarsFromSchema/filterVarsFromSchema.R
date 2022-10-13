# File:       getVarsFromSchema.R
# Decription: Read Module 1 Schema from excel file. Filter variables and export 
#             to M2-variables.csv and M2-lists.js files.
# Author:     Jake Peters
# Date:       October 2022

library("readxl")
library("dplyr")

## Get schema data for Version 1 and Version 2
excel_file <- "/Users/petersjm/Documents/dataFlattening/autoFlattening/module_1_schemas.xlsx"
df_V1 <- read_excel(excel_file, sheet = "M1V1")
df_V2 <- read_excel(excel_file, sheet = "M1V2")

# Make all columns factors
df_V1[] <- lapply(df_V1, factor) # the "[]" keeps the dataframe structure
df_V2[] <- lapply(df_V2, factor) 

## Filter out RECORDS and keys
df_V1_filt  <- df_V1 %>% filter(!grepl("__key__", fullname)) %>% filter(!grepl("RECORD", type_))
df_V2_filt  <- df_V2 %>% filter(!grepl("__key__", fullname)) %>% filter(!grepl("RECORD", type_))

## Get list of variables that do not repeat (destiined for M2*-variables.csv)
df_V1_vars  <- df_V1_filt %>% filter(!grepl("REPEATED", mode))
df_V2_vars  <- df_V2_filt %>% filter(!grepl("REPEATED", mode))

## Get variables that do repeated (destined for M2*-lists.js)
df_V1_lists <- df_V1_filt %>% filter(grepl("REPEATED", mode))
df_V2_lists <- df_V2_filt %>% filter(grepl("REPEATED", mode)) 

## View Results of filtering
print(paste("Num. V1 vars:", eval(nrow(df_V1_vars)), sep=" "))
print(paste("Num. V2 vars:", eval(nrow(df_V2_vars)), sep=" "))
print(paste("Num. V1 lists:", eval(nrow(df_V1_lists)), sep=" "))
print(paste("Num. V2 lists:", eval(nrow(df_V2_lists)), sep=" "))
head(df_V1_vars)
head(df_V2_vars)
head(df_V1_lists)
head(df_V1_lists)

## Export variables to M2V*-variables.csv file for queryGenerator
write.table(df_V1_vars$fullname, file="M1V1-variables.csv", 
            col.names=FALSE, row.names=FALSE, quote=FALSE, sep="," )
write.table(df_V2_vars$fullname, file="M1V2-variables.csv", 
            col.names=FALSE, row.names=FALSE, quote=FALSE, sep="," )

## Generate M2V*-lists.js file for queryGenerator

# Define strings to open and close M2V*-lists.js files
opening_line <- "const pathToConceptIdList = {"
closing_line <- "};"
ending_line  <- "module.exports=pathToConceptIdList;"

# Write lines for V1
fidM1V1 = "M1V1-lists.js" # destination file
write(opening_line, file=fidM1V1, append=FALSE)
for (var_name in df_V1_lists$fullname) {
      var_line <- paste("'", var_name, "': [],", sep="")
      write(var_line, file=fidM1V1, append=TRUE)
    }
write(closing_line, file=fidM1V1, append=TRUE)
write(ending_line, file=fidM1V1, append=TRUE)

# Write lines for V2
fidM1V2 = "M1V2-lists.js" # destination file
write(opening_line, file=fidM1V2, append=FALSE)
for (var_name in df_V2_lists$fullname) {
      var_line <- paste("'", var_name, "': [],", sep="")
      write(var_line, file=fidM1V2, append=TRUE)
    }
write(closing_line, file=fidM1V2, append=TRUE)
write(ending_line,  file=fidM1V2, append=TRUE)
