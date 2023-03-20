# Reduce memory build up.
#rm(list=ls())

#Load Functions 
source('functions.R')

# Load rest of packages 
library(yaml)
library(readr)
library(googleCloudStorageR)
library(parallel)

# Get Environment Variables for Cloud Run service.
clientToken <- Sys.getenv('CLIENT_SECRET_PATH')
serviceAccount <-  Sys.getenv('SERVICE_ACCOUNT_PATH')
bucket <- Sys.getenv('BUCKET')
desiredNumObjects <- as.numeric(Sys.getenv('NUMBER_OF_CONFIGURATIONS'))


# Check if desired number of configuration files is 1 or smaller or doesn't exist
if (desiredNumObjects <= 1 || desiredNumObjects == '' ) {
  # Do not run code in parallel - one config file
  
  # Authenticate to Google Cloud Storage
  gcs_auth(json_file = serviceAccount, token = clientToken)
  
  # Get the name of yaml config file 
  configFile <- (gcs_list_objects(bucket, detail = 'summary',  'config/'))$name
  configFile <- configFile[grep('.yaml$',configFile) ]
  
  # Get Updated Config File from GCS
  fetch <- gcs_get_object(
    object_name = paste0('gs://',bucket,'/',configFile), 
    saveToDisk = configFile,
    overwrite = TRUE
  )
  
  # Read the config file saved to disk from GCS
  config <- read_yaml(configFile)
  
  # Get Universal Analytics data and send to BigQuery
  ua_past_data(
    config = config,
    auth = auth(clientToken, serviceAccount),
    slow_fetch = config$options$slow_fetch,
    anti_sample_batches = config$options$anti_sample_batches
  )
  
  
} else {
  # Execute Cloud Run Service in parallel
  
  # Authenticate to Google Cloud Storage.
  gcs_auth(json_file = serviceAccount, token = clientToken)
  
  # Get the names of yaml config files 
  configFiles <- (gcs_list_objects(bucket, detail = 'summary',  'config/'))$name
  configFiles <- configFiles[grep('.yaml$',configFiles) ]
  
  # Total number of config files
  numObjects <- length(configFiles)
  
  # Check if the number of objects has reached the desired threshold 
  if (numObjects >= desiredNumObjects) {
    # Run parallel processing to get past UA data. 
    cl <- makeCluster(parallel::detectCores())
    clusterExport(cl, ls())
    
    # Define the function that will run on each instance
    run_instance <- function(config_file) {
      
      # Remove all objects from the R environment
      #rm(list = ls())
      
      library(googleCloudStorageR)
      library(yaml)
      library(readr)
      
      library(googleAuthR)
      library(googleAnalyticsR)
      library(bigrquery)
      
      #### MAIN ####

      gcs_auth(json_file = serviceAccount, token = clientToken)
      
      # Get Updated Config File from GCS
      fetch <- googleCloudStorageR::gcs_get_object(
        object_name = paste0('gs://',bucket,'/',config_file), 
        saveToDisk = config_file,
        overwrite = TRUE
      )
      

      config <- read_yaml(config_file)
      
      startDate <- ua_past_data(
        config,
        auth = auth(clientToken, serviceAccount),
        slow_fetch = config$options$slow_fetch,
        anti_sample_batches = config$options$anti_sample_batches
      )  
      
    }
    
    # Run the function in parallel using the cluster
    results <- parSapply(cl, configFiles, run_instance)
    
    # Stop the cluster
    stopCluster(cl)
    
  } else {
    print('Have not reached the desired number of config files.')
  }
  

}












