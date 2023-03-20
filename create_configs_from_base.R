# Load the yaml and stringr packages
library(yaml)
library(stringr)

SOURCE_FILE <- '<Choose a source (parallel or sequential) Fields File>'
BASE_CONFIG_FILE <- '<Name of the base config file>'
REPLACE_CLIENT_ID_WITH_CD_INDEX <- '<Client ID CD Index Number>'

# Load the source file based on the chosen source
source(SOURCE_FILE)

# Read in the base configuration from a yaml file
baseConfig <- read_yaml(BASE_CONFIG_FILE)

# If the source is sequential...
if (grepl('sequential', SOURCE_FILE)) {
  
  # Set up the base view configuration
  baseView <- list(
    view_id =  baseConfig$ga$views[[1]]$view_id,
    dimensions = NULL,
    metrics = NULL,
    order = c('date', 'ASCENDING')
  )
  
  views <- list()
  
  # Loop through each table in the dimensionsAndMetrics list
  for (eachTable in dimensionsAndMetrics ) {
    # Create a new view configuration for each table
    eachView <- baseView
    if (!is.null(REPLACE_CLIENT_ID_WITH_CD_INDEX)) {
      eachView$dimensions <- str_replace(
        eachTable$dimensions,
        'clientId',
        paste0('dimension', REPLACE_CLIENT_ID_WITH_CD_INDEX)
      )
    } else {
      eachView$dimensions <- eachTable$dimensions 
    }
    eachView$metrics <- eachTable$metrics
    
    # Add the view configuration to the views list
    views <- append(views, list(eachView))
  }
  
  # Update the base configuration with the views list
  config <- baseConfig
  config$ga$views <- views 
  
  # Update the list of BigQuery tables with the names of the tables from dimensionsAndMetrics
  config$gcp$bq$table$names <- names(dimensionsAndMetrics)
  
  # Write the updated configuration to a yaml file
  write_yaml(config, 'config/config.yaml')
  
} else if ( grepl('parallel', SOURCE_FILE)) { # If the source is parallel...
  
  # Loop through each table in the dimensionsAndMetrics list
  for (eachTable in 1:length(dimensionsAndMetrics)) {
    
    # Create a new configuration for each table
    eachConfig <- baseConfig
    eachTableName <- names(dimensionsAndMetrics[[eachTable]])
    
    # Update the view configuration with the dimensions and metrics for the current table
    if (!is.null(REPLACE_CLIENT_ID_WITH_CD_INDEX)) {
      eachConfig$ga$views[[1]]$dimensions <- str_replace(
        dimensionsAndMetrics[[eachTable]][[eachTableName]]$dimensions,
        'clientId',
        paste0('dimension', REPLACE_CLIENT_ID_WITH_CD_INDEX)
      )
    } else {
      eachConfig$ga$views[[1]]$dimensions <- dimensionsAndMetrics[[eachTable]][[eachTableName]]$dimensions  
    }
    eachConfig$ga$views[[1]]$metrics <- dimensionsAndMetrics[[eachTable]][[eachTableName]]$metrics
    
    # Update the list of BigQuery tables with the name of the current table
    eachConfig$gcp$bq$table$names <- c(eachTableName)
    
    # Write the updated configuration to a yaml file named after the current table
    write_yaml(
      eachConfig,
      paste0('config/', eachTableName, '.yaml')
    )
    
  }
  
}