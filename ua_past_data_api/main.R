source('functions.R')

# General
library(yaml)

library(googleCloudStorageR)

##### Global Vars ####
# Read initial config file
configFile <- 'config/config.yaml'
config <- read_yaml(configFile)

# Get base (authentication) info from config 
clientToken <- config$auth_path$client_token
serviceAccount <- config$auth_path$service_account
bucket <- config$gcp$gcs$bucket_name

#### GCS ####
# Authenticate to Google Cloud Storage
gcs_auth(json_file = serviceAccount, token = clientToken)

# Get Updated Config File from GCS
fetch <- gcs_get_object(
  object_name = paste0('gs://',bucket,'/',configFile), 
  saveToDisk = configFile,
  overwrite = TRUE
)

#### Run Main ####
config <- read_yaml(configFile) #Refresh config

# Get Universal Analytics data and send to BigQuery
ua_past_data(
  auth = auth(clientToken, serviceAccount),
  slow_fetch = config$options$slow_fetch,
  anti_sample_batches = config$options$anti_sample_batches
)
