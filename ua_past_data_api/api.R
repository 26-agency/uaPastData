#### PACKAGES ####
source('functions.R')
# APIS
#library(googleCloudStorageR)
library(googleCloudRunner)

# General
library(yaml)


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