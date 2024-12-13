## 0. Load dependencies and define parameters ##################################
library(jsonlite)
library(bigrquery)
source('flatten.R')

# GCP Project IDs
projects <- list(dev  = "nih-nci-dceg-connect-dev", 
                 stg  = "nih-nci-dceg-connect-stg-5519", 
                 prod = "nih-nci-dceg-connect-prod-6d04")

# BQ Source Tables
tables   <- list(
  b    = "biospecimen",          bs   = "bioSurvey_v1", 
  cbs  = "clinicalBioSurvey_v1", ms   = "menstrualSurvey_v1",
  m1v2 = "module1_v2",           m2v2 = "module2_v2",
  #m1v1 = "module1_v1",          m2v1 = "module2_v1",           
  m3   = "module3_v1",           m4   = "module4_v1",
  p    = "participants",         cs19 = "covid19Survey_v1",
  n    = "notifications",        k    = "kitAssembly",
  mw   = "mouthwash_v1",         co   = "cancerOccurrence",
  prom = "promis_v1",            cshs = "cancerScreeningHistorySurvey",
  ex   = "experience2024")

########### DEFINE PARAMETERS ##################################################
tier    <- 'prod' # GCP tiers to update sch. queries for
tables  <- tables
run_now <- FALSE # Set to true if you want to both sch. and run query
################################################################################

# Authenticate to BigQuery
project <- projects[[tier]] # project to get schema from (can only select one)
bigrquery::bq_auth()

for (table in tables) {
  flatten(project, table, tier)
}