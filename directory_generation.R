#11/9, R. Sansale
#issue #42, AnalyticSphere/flatteningRequests 
#generating separate stg/dev/prod directories
#for each survey contained in the queryGenerators folder
#this only needs to be run once in order to generate
#all of the new directories that have been requested
#the first loop copies and creates directories
#the second loop deletes extraneous files
#these are separate so i could check my work

#identify names of each survey
base_dir <- "queryGenerators/FlatConnect_queries"
survey <- basename(list.dirs(base_dir, recursive =FALSE))
new_folders <- c("stg", "prod", "dev")

copy_files <- c("-lists.json", "-query_generator.js", "-variables.csv")
for(i in 1:length(survey)){
  surv <- survey[i]
  base_path <- paste0(base_dir,"/", surv,"/")
  #create 3 new folders
  dir.create(paste0(base_path, "stg"))
  dir.create(paste0(base_path, "prod"))
  dir.create(paste0(base_path, "dev"))
  
  #copy files according to the folder they belong to
  cp_files <- paste0(base_path,paste0(surv,copy_files))
  cp_files <- c(cp_files, paste0(base_path, "query-config.json"))
  file.copy(cp_files, paste0(base_path, "stg"))
  file.copy(cp_files, paste0(base_path, "dev"))
  file.copy(cp_files, paste0(base_path, "prod"))
}


#removing files in the base_path
#this couldve been done in 1 loop but i wanted to test
#the output before deleting the files in base_path
for(i in 1:length(survey)){
  surv <- survey[i]
  base_path <- paste0(base_dir,"/", surv, "/")
  
  #copy files according to the folder they belong to
  rm_files <- paste0(base_path,paste0(surv,copy_files))
  rm_files <- c(rm_files, paste0(base_path, "query-config.json"))
  file.remove(rm_files)
  base_path <- paste0(base_dir,"/", surv)
  sql_files <- list.files(path = base_path, pattern = "\\.sql$", full.names = TRUE)
  file.remove(sql_files)
}





