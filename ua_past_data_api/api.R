#### PACKAGES ####
source('functions.R')
# APIS
#library(googleCloudStorageR)
library(googleCloudRunner)

# General
#library(yaml)

bucket <- 'ua_past_data'

project_id <- 'datascience-twentysixdigital'
script_name = "ua-past-data"
image_name = paste0("gcr.io/",project_id,'/',script_name,"-r-docker")

service_account <- 'auth/datascience_service_account.json'
timeout <- 600

#' @get /
#' @html
function(){
  
 
  
  #source('main.R')

  paste0("<html><h1>CHEESE</h1></html>")
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
  
  #Global vars ####
  #service_account <- 'datascience_service_account.json'
  #client_token <- 'ga_client_secret.json'
  #bucket <- "ua_past_data"
  
  pub <- function(x){
    
    
    source('main.R')
    
  }
  
  # Get UA GA Data and send to BQ
  googleCloudRunner::cr_plumber_pubsub(message, pub)
  
  
  
  
}