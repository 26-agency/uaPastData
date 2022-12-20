#### PACKAGES ####
source('functions.R')
# APIS
#library(googleCloudStorageR)
library(googleCloudRunner)

# General
library(yaml)

#### Global Vars ####
configFile <- 'config/config.yaml'
config <- read_yaml(configFile)

bucket <- config$gcp$gcs$bucket_name
project_id <- config$gcp$project_id
service_account <- config$auth_path$service_account

script_name = "ua-past-data"
image_name = paste0("gcr.io/",project_id,'/',script_name,"-r-docker")


timeout <- 600

#' @get /
#' @html
function(){
  paste0("<html><h1>UA PAST DATA WORKS</h1></html>")
}


#' @get /hello
#' @html
function(){
  "<html><h1>hello world</h1></html>"
}


#' Recieve pub/sub message
#' @post /pubsub
#' @param message a pub/sub message
function(message=NULL){
  
  pub <- function(x){
    source('main.R')
  }
  
  # Get UA GA Data and send to BQ
  googleCloudRunner::cr_plumber_pubsub(message, pub)
  
}