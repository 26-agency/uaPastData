source('functions.R')

# General
library(yaml)

library(googleCloudStorageR)

##### Global Vars ####
clientToken <- 'auth/ga_client_secret.json'
serviceAccount <- 'auth/datascience_service_account.json'

#### Pubsub Auth ####

gcs_auth(json_file = serviceAccount, token = clientToken)

configFile <- 'config/config.yaml'
fetch <- gcs_get_object(
  object_name = paste0('gs://ua_past_data/',configFile), 
  saveToDisk = configFile,
  overwrite = TRUE
)

config <- read_yaml(configFile)


ua_past_data(
  auth = auth(clientToken, serviceAccount),
  slow_fetch = config$options$slow_fetch,
  anti_sample_batches = config$options$anti_sample_batches
)
