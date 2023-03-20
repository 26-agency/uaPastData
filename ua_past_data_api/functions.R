library(googleAuthR)
library(googleAnalyticsR)
library(bigrquery)
library(lubridate)

# This function retrieves historical Google Analytics data for each view specified in the config file, 
# and sends it to its corresponding BigQuery dataset. The function can retrieve data either quickly or 
# slowly depending on the value of `slow_fetch`, which defaults to NULL. 
# It takes in the following arguments:
#   - config: a list object containing the configuration parameters for the function
#   - auth: an object containing the authentication details for accessing Google Analytics and BigQuery
#   - slow_fetch: a logical indicating whether to perform a slow fetch or not
#   - anti_sample_batches: a numeric indicating the number of anti-sample batches to be used in the slow fetch
ua_past_data <- function(
    config,    
    auth,      
    slow_fetch  = NULL,  
    anti_sample_batches = NULL
){
  
  # This is a helper function that retrieves GA data for a single view and sends it to the specified BQ table.
  # It takes in the following arguments:
  #   - config: a list object containing the configuration parameters for the function
  #   - ga_view: an object containing the Google Analytics view details
  #   - bq_table_name: a character string specifying the name of the BigQuery table to write to
  #   - fetch: a character string specifying the type of fetch to perform (either 'fast' or 'slow')
  single_table_ua_past_data  <- function (config, ga_view, bq_table_name, fetch) {
    
    if (fetch == 'fast') {
      viewId <- ga_view$view_id
      
      # Check to see if the BQ table already exists and query the date range. Change config dates according to this
      #works really well with working against GA API breaking with pubsub
      startDate <- bq_check_and_reset_table_date_range(
        project_id=config$gcp$project_id, 
        dataset=config$gcp$bq$dataset$name,
        view_id = viewId,
        start_date=config$time$date_range[1], 
        table=bq_table_name )
      
      if (!isFALSE(startDate))  {
        # Get Ga data
        gaData <- get_ga_data(
          view_id = viewId,
          date_range = c(startDate, config$time$date_range[2]),
          dimensions = ga_view$dimensions,
          metrics = ga_view$metrics,
          order = ga_view$order
        )
        
        send_data_to_bq(
          gaData,
          dataset = config$gcp$bq$dataset$name,
          view_id = viewId,
          project_id = config$gcp$project_id,
          write_disposition = config$gcp$bq$table$write_disposition,
          location = config$gcp$bq$dataset$location,
          expiration = config$gcp$bq$dataset$expiration_days,
          table = bq_table_name
        )  
      } else {
        print(paste0(bq_table_name, ' The Start date already exists for this BQ table.'))
      }
      
    } else { # fetch== 'slow'
      
      viewId <- ga_view$view_id
      startDate <-config$time$date_range[1]
      
      latestStartDate <- bq_check_and_reset_table_date_range(
        project_id=config$gcp$project_id, 
        dataset=config$gcp$bq$dataset$name, 
        start_date=startDate,
        view_id = viewId, 
        table=bq_table_name)
      
      print(paste('Current Starting Date in table', bq_table_name, 'is', latestStartDate, 'Actual Start date is', startDate))
      
      if (!(latestStartDate==startDate) || is.null(latestStartDate))  {
        get_ga_data_and_send_to_bq_batches(
          view_id = viewId,
          latest_date = latestStartDate,
          date_range = c(config$time$date_range[1], config$time$date_range[2]),
          dimensions = ga_view$dimensions,
          metrics = ga_view$metrics,
          order = ga_view$order,
          
          dataset = config$gcp$bq$dataset$name,
          project_id = config$gcp$project_id,
          write_disposition = config$gcp$bq$table$write_disposition,
          location = config$gcp$bq$dataset$location,
          expiration = config$gcp$bq$dataset$expiration_days,
          table = bq_table_name,
          anti_sample_batches = anti_sample_batches
          
          
        ) 
      } else {
        print(paste0(bq_table_name, ' The Start date already exists for this BQ table.'))
      }
      
    }
    
    return(latestStartDate)
    
  }
  
  
  # Authenticate to GA and BQ
  auth
  
  #Decide whether to slow fetch or not
  if (typeof(slow_fetch) == 'NULL' & typeof(anti_sample_batches) == 'NULL') {
    if( is.null(slow_fetch) & is.null(anti_sample_batches) ) {
      fetch = 'fast'
    } else {fetch = 'slow'}  
  } else if (
    typeof(slow_fetch) == 'NULL' & 
    typeof(as.numeric(anti_sample_batches)) == 'double'
  ){
    if( is.null(slow_fetch) & !anti_sample_batches) {
      fetch = 'fast'
    } else {fetch = 'slow'}
  } else if (
    typeof(slow_fetch) == 'logical' & 
    typeof(anti_sample_batches) == 'NULL'
  ) {
    if( !slow_fetch & is.null(anti_sample_batches)) {
      fetch = 'fast'
    } else {fetch = 'slow'}
  } else if (
    typeof(slow_fetch) == 'logical' & 
    typeof(as.numeric(anti_sample_batches)) == 'double'
  ) {
    if( !slow_fetch & !anti_sample_batches) {
      fetch = 'fast'
    } else {fetch = 'slow'}
  }
  
  
  # Decide how to get and send data based on config.
  viewsInfo <- config$ga$views #Information about each view 
  viewIds <- c() # Get all the view Ids in a vector
  for (eachView in viewsInfo) { viewIds <- append(viewIds, eachView$view_id) }
  bqTables <- config$gcp$bq$table$names # BQ dataset names
  if (length(viewsInfo) == length(bqTables)) {

    # Get GA data from each view and send to each BQ dataset retrospectively to the config    
    for (eachTable in 1:length(config$ga$views)) {
      
      eachBqDataset <- config$gcp$bq$table$names[[eachTable]]
      eachView <- config$ga$views[[eachTable]]
     
      output <- single_table_ua_past_data(
        config,
        ga_view = eachView, 
        bq_table_name = eachBqDataset, 
        fetch= fetch
      )
      
    }
    
  } else if (var(viewIds) == 0 && length(bqTables) == 1) {
    bqDataset <- bqTables   
    # Get GA data and send to a BQ dataset  for each View   
    for (eachTable in 1:length(config$ga$views)) {
      
      eachView <- config$ga$views[[eachTable]]

      output <- single_table_ua_past_data(
        config,
        ga_view = eachView, 
        bq_table_name = bqDataset, 
        fetch = fetch
      )
    
    }
    
    
  } else {
    stop(
      'Either a) the number of GA views for API pull must be(retrospectively) 
      the same as the number of BQ datasets or b) there is only one BQ dataset 
      with multiple views, all having the same View ID.'
    )
  }
  
  return(output)
 
  
}


auth <- function(client_token,service_account){
  tryCatch({
    googleAuthR::gar_set_client(
      json = file.path(client_token),
      scopes = c("https://www.googleapis.com/auth/analytics.readonly")
    )
    googleAuthR::gar_auth_service(json_file = file.path(service_account))
    
    # BQ
    bigrquery::bq_auth(path = file.path(service_account))
    
  },error = function(e) {
    print(paste("Something went wrong API Authentication:", e))
    stop()
  })
}


# Check if the specified BigQuery table exists and query the date range.
# If the table exists, get the furthest date from it and use it to set the start date.
# This function is useful when working with the Google Analytics API and Pub/Sub.
bq_check_and_reset_table_date_range <- function(
    project_id, 
    dataset, 
    view_id,
    start_date, 
    table=NULL ) {
  
  # Determine the table name based on the specified view ID and, if provided, the table name.
  if (is.null(table)) {
    table <- view_id  
  } else {
    table <- paste(view_id, table, sep = '_')
  }
  
  # Create variables for the project, dataset, and table names.
  project.dataset <- paste0(project_id,'.',dataset)
  project.dataset.table <- paste0(project.dataset,'.',table)
  
  # Check if the table exists in BigQuery.
  if (bq_table_exists(project.dataset.table)) {
    # If the table exists, get the furthest date from it.
    sql = paste(
      "SELECT date FROM `",project.dataset.table,"` GROUP BY date ORDER BY date LIMIT 1 ",
      sep = ''
    )
    latestDate <- bq_table_download(bq_project_query(project_id,sql))
    latestDate <- latestDate$date  
  } else {
    # If the table doesn't exist, set the latest date to NULL.
    print(paste('BQ Table', table, 'does not exist yet. Setting latest Date to NULL'))
    latestDate = NULL       
  }
  
  # Return the latest date obtained from the table or NULL if the table doesn't exist.
  return(latestDate)
}



get_ga_data <- function(
    view_id, 
    date_range = c(start_date, end_date), 
    dimensions,
    metrics,
    order) {# "auto" default, or set to number of days per batch. 1 = daily.
  
  #Get GA Data (this is out of loop for a more efficient call but will break if there are more than 10 metrics because of API limits)
  start_date <- date_range[1]
  end_date <- date_range[2]
    gaData <- google_analytics(
      viewId = view_id,
      date_range = c(start_date, end_date),
      dimensions = dimensions,
      metrics = metrics,
      order = list(
        order_type(order[1], order[2])
      ),
      anti_sample = TRUE
    ) 
  
  
  
}


# Uploads the specified data frame to BigQuery and creates a new dataset or table if necessary.
send_data_to_bq <- function(
    df,
    dataset,
    view_id,
    project_id,
    write_disposition,
    location,
    expiration,
    table = NULL) {
  
  # Set the partitioning type to MONTH and field to "date".
  partitioning <- list(type = "MONTH", field = "date") 
  
  # Determine the table name based on the specified view ID and, if provided, the table name.
  if (is.null(table)) {
    table <- view_id  
  } else {
    table <- paste(view_id, table, sep = '_')
  }
  
  # Create variables for the project, dataset, and table names.
  project.dataset <- paste0(project_id,'.',dataset)
  project.dataset.table <- paste0(project.dataset,'.',table)
  
  tryCatch(
    {
      # Create the dataset and table if they don't exist in BigQuery.
      if ( # if bq dataset and table don't exist
        !(bq_dataset_exists(project.dataset)) && 
        !(bq_table_exists(project.dataset.table))) {  
        
        # Set the default table expiration time to the specified value.
        expiration = expiration * 8.64e+7
        
        # Create the dataset with the specified location and default table expiration time.
        bq_dataset_create(
          project.dataset,
          location,
          defaultTableExpirationMs = expiration
        )
        
        # Create the table with the specified schema and partitioning.
        bq_table_create(
          project.dataset.table,
          fields = as_bq_fields(df),
          timePartitioning = partitioning
        )  
        
      } else if ( # if bq dataset exists and table does not
        bq_dataset_exists(project.dataset) && 
        !bq_table_exists(project.dataset.table)) {
        
        # Create the table with the specified schema and partitioning.
        bq_table_create(
          project.dataset.table,
          fields = as_bq_fields(df),
          timePartitioning = partitioning
        )  
      }
    },
    error = function(e){
      # Handle any errors that occur while managing the dataset and tables.
      print(paste("Something went wrong with managing BQ dataset/tables:", e))
    }
  )
  
  #TODO - REMOVE head() data changing part when ready
  #aprioriRulesDf <- head(aprioriRulesDf)
  
  tryCatch(
    {
      # Upload the data to the table in BigQuery.
      bq_table_upload(
        project.dataset.table,
        values = df,
        create_disposition = "CREATE_IF_NEEDED",
        write_disposition=write_disposition,
        fields = as_bq_fields(df),
        configuration =list(
          timepartitioning = partitioning
        )
      )
    },
    error = function(e){
      # Handle any errors that occur while uploading data to BigQuery.
      print(paste("Something went wrong uploading BQ data:", e))
    }
  )
}


google_analytics_try <- function(
    viewId,
    date_range = NULL,
    metrics = NULL,
    dimensions = NULL,
    order = NULL,
    anti_sample = FALSE,
    anti_sample_batches = "auto",
    slow_fetch = FALSE,
    rows_per_call = 10000L ) {
  
  
  
  tryCatch({
    print('Try to run request with slow fetch and 10000 rows per call')
    gaData <- googleAnalyticsR::google_analytics(
      viewId = viewId,
      date_range = date_range,
      dimensions = dimensions,
      metrics = metrics,
      order = order,
      anti_sample = TRUE,
      slow_fetch = TRUE,
      rows_per_call = 10000L
    )
  }, error = function(error){
    
    errorMessage <- error$message
    print(errorMessage)
    if (errorMessage %in% c("internalServerError", "backendError")) {
      
      print('Try to run request with no slow fetch')
      tryCatch({
        gaData <- googleAnalyticsR::google_analytics(
          viewId = viewId,
          date_range = date_range,
          dimensions = dimensions,
          metrics = metrics,
          order = order,
          anti_sample = TRUE,
          slow_fetch = FALSE
        )
      }, error = function(error){
        errorMessage <- error$message
        print(errorMessage)
        if (errorMessage %in% c("internalServerError", "backendError")) {
          print('Try to run request by decreasing row Requests')
          for (rowPerCallsHalved in c(5000L, 2500L, 1250L, 625L, 313L)) {
            tryCatch({
              gaData <- googleAnalyticsR::google_analytics(
                viewId = viewId,
                date_range = date_range,
                dimensions = dimensions,
                metrics = metrics,
                order = order,
                anti_sample = TRUE,
                slow_fetch = TRUE,
                rows_per_call = rowPerCallsHalved
              )
              break
            })
          }
          
        }
      }) 
    }
  })
  if(exists("gaData")){
    return(gaData)
  }else{
    print("There has been an error, the request never succeeded.")
  }
  
  
}

# Function to get data from Google Analytics API and send it to BigQuery in batches
#   - view_id:  Google Analytics view ID
#   - latest_date: Latest date of data available in BigQuery
#   - date_range: Date range to retrieve data from Google Analytics
#   - dimensions:  Dimensions to retrieve from Google Analytics
#   - metrics: Metrics to retrieve from Google Analytics
#   - order: Order of data retrieved from Google Analytics
#   - dataset: BigQuery dataset to send data to
#   - project_id: Project ID for BigQuery
#   - write_disposition: Write disposition for BigQuery
#   - location: Location of BigQuery dataset
#   - expiration: Expiration time for BigQuery table
#   - table: BigQuery table to send data to
#   - anti_sample_batches: Number of days per batch for Google Analytics data retrieval
get_ga_data_and_send_to_bq_batches <- function(
    view_id, 
    latest_date,
    date_range = c(start_date, end_date), 
    dimensions,
    metrics,
    order,
    dataset,
    project_id,
    write_disposition,
    location,
    expiration,
    table = NULL,
    anti_sample_batches = 1
  
) {
  
  
  startDate <- as.character(date_range[1])
  endDate <- as.character(date_range[2])

  print(paste('Start date:', startDate, 'is class', class(startDate)))
  print(paste('End date:', endDate, 'is class', class(endDate)))
  if (is.null(latest_date)){
    latest_date <- endDate
    resetDate = 0
  } else{
    writeDisposition <- 'WRITE_APPEND'
    resetDate = 1
  }
  print(paste('Lateest Date:', latest_date, 'is class', class(latest_date)))
  
  #totalDays <- (endDate - startDate)[[1]] + 1
  
  totalDays = as.double(difftime(lubridate::ymd(latest_date)- 1,
                                 (lubridate::ymd(startDate) - 1),
                                 units = "days"))
  print('calculated total days left in requests')
  print(totalDays)
  
  #print(anti_sample_batches > totalDays)
  if (anti_sample_batches > totalDays) {
    stop( #Maybe this shouldnt be a stop?
      "ga_anti_sample_batches days cannot be bigger than the total number 
          of days in 'date_range")
  }
  
  gaData <- NULL
  for (batchPeriod in seq(from=anti_sample_batches, 
                          to=totalDays, 
                          by=anti_sample_batches)) {
   
    
    #gaRequestStartDate <- (ymd(endDate) - days(daysbatchPeriod) + days(1))
    gaRequestStartDate <- (lubridate::ymd(latest_date) - resetDate -  batchPeriod + 1)
    #gaRequestEndDate <- endDate - batchPeriod + anti_sample_batches
    gaRequestEndDate <- (lubridate::ymd(latest_date) - resetDate - batchPeriod + anti_sample_batches)
    print('Actual GA Request Start Date:') 
    print(gaRequestStartDate)
    print('Actual GA Request End Date:') 
    print(gaRequestEndDate)
    
   
    gaData <- google_analytics_try(
      viewId = view_id,
      date_range = c(gaRequestStartDate, gaRequestEndDate),
      dimensions = dimensions,
      metrics = metrics,
      order = list(
        order_type(order[1], order[2])
      ),
      anti_sample = TRUE,
      slow_fetch = TRUE
    )
    
    print('Got GA Data with col names:')
    print(paste(names(gaData), collapse = ','))
    
    if (!is.null(gaData)) {
      
      if (batchPeriod == 1) { #BQ data already exists and  write di is TRUNCATE?
        
        if (latest_date == endDate) {
          writeDisposition = write_disposition  
        } else {
          writeDisposition = 'WRITE_APPEND'
        }
        
      } else {
        writeDisposition = 'WRITE_APPEND'
      } 
      
      print('Send ga data to bq')
      send_data_to_bq(
        gaData,
        dataset,
        view_id,
        project_id,
        write_disposition=writeDisposition,
        location,
        expiration,
        table = table
      )  
    } else {
      print(paste0(
          'GA Data Request retured NULL for ', dataset,' BQ dataset. 
          There is likely no data  for time perod:', 
          gaRequestStartDate, ' - ', gaRequestEndDate))
    }
      
  }
}  
  
  

