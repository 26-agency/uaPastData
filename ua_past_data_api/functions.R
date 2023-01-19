# API
library(googleAuthR)
library(googleAnalyticsR)
library(bigrquery)


ua_past_data <- function(
    auth, 
    slow_fetch  = NULL,
    anti_sample_batches = NULL
){
  
  # Run a single instance of getting UA data and sending to BQ table.
  "
  ga_view = config$ga$views[[1]]
  bq_table_name = config$gcp$bq$table$names[1]
  fetch = 'slow'
  anti_sample_batches = config$options$anti_sample_batches
  "
  
  single_table_ua_past_data  <- function (ga_view, bq_table_name, fetch) {
    
    
    if (fetch == 'fast') {
      viewId <- ga_view$view_id
      
      # Check to see if the BQ table already exists and query the date range. Change config dates according to this - works really well with working against GA API breaking with pubsub
      startDate <- bq_check_and_reset_table_date_range(
        project_id=config$gcp$project_id, 
        dataset=config$gcp$bq$dataset$name,
        view_id = viewId,
        start_date=config$time$date_range[1], 
        table=bq_table_name )
      
      
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
    } else { # fetch== 'slow'
      
      viewId <- ga_view$view_id
      
      
      startDate <- bq_check_and_reset_table_date_range(
        project_id=config$gcp$project_id, 
        dataset=config$gcp$bq$dataset$name, 
        start_date=config$time$date_range[1],
        view_id = viewId, 
        table=bq_table_name)
      
      
      get_ga_data_and_send_to_bq_batches(
        view_id = viewId,
        date_range = c(startDate, config$time$date_range[2]),
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
    }
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
      
      single_table_ua_past_data(
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
      
      single_table_ua_past_data(
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


# Check to see if the BQ table already exists and query the date range. 
# Change config dates according to this - works really well with working 
# against GA API breaking with pubsub
bq_check_and_reset_table_date_range <- function(
    project_id, 
    dataset, 
    view_id,
    start_date, 
    table=NULL ) {
  
  
  if (is.null(table)) {
    table <- view_id  
  } else {
    table <- paste(view_id, table, sep = '_')
  }
  project.dataset <- paste0(project_id,'.',dataset)
  project.dataset.table <- paste0(project.dataset,'.',table)
  
  tryCatch(
    {
      
      if (bq_table_exists(project.dataset.table)) { #If table exists 
        
        #Get the furthest away date form BQ
        sql = paste(
          "
            SELECT date 
            FROM `",project.dataset.table,"` 
            GROUP BY date
            ORDER BY date LIMIT 1 
          ",
          sep = ''
        )
        latestDate <- bq_table_download(bq_project_query(project_id,sql))
        
        startDate <- latestDate$date
        
      } else {
        startDate <- start_date        
      }
      
      return(startDate)
      
    },
    error = function(e){
      print(paste("Something went wrong with checking BQ table dates", e))
    }
  )
  
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


send_data_to_bq <- function(
    df,
    dataset,
    view_id,
    project_id,
    write_disposition,
    location,
    expiration,
    table = NULL) {
  
  partitioning <- list(type = "MONTH", field = "date") 
  if (is.null(table)) {
    table <- view_id  
  } else {
    table <- paste(view_id, table, sep = '_')
  }
  
  project.dataset <- paste0(project_id,'.',dataset)
  project.dataset.table <- paste0(project.dataset,'.',table)
  
  tryCatch(
    {
      #Create dataset and or table if necc then upload data to bq
      if ( # if bq dataset and table don't exist
        !(bq_dataset_exists(project.dataset)) && 
        !(bq_table_exists(project.dataset.table))) {  
        
        expiration = expiration * 8.64e+7
        bq_dataset_create(
          project.dataset,
          location,
          defaultTableExpirationMs = expiration
          #configuration =list(
          #  timepartitioning = partitioning
          
          #)
        )
        bq_table_create(
          project.dataset.table,
          fields = as_bq_fields(df),
          timePartitioning = partitioning
        )  
        
      } else if ( # if bq dataset exists and table does not
        bq_dataset_exists(project.dataset) && 
        !bq_table_exists(project.dataset.table)) {
        
        bq_table_create(
          project.dataset.table,
          fields = as_bq_fields(df),
          timePartitioning = partitioning
        )  
      }
    },
    error = function(e){
      print(paste("Something went wrong with managing BQ dataset/tables:", e))
    }
  )
  #TODO - REMOVE head() data changing part when ready
  #aprioriRulesDf <- head(aprioriRulesDf)
  tryCatch(
    {
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
      print(paste("Something went wrong uploading BQ data:", e))
    }
  )
}


get_ga_data_and_send_to_bq_batches <- function(
    
  view_id, 
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
  
  
  
  startDate <- as.Date(date_range[1])
  endDate <- as.Date(date_range[2])
  totalDays <- (endDate - startDate)[[1]] + 1
  if (anti_sample_batches > totalDays) {
    stop(
      "'ga_anti_sample_batches days cannot be bigger than the total number 
          of days in 'date_range'")
  }
  
  
  gaData <- NULL
  daysSequence <- seq(
    from=anti_sample_batches, 
    to=totalDays, 
    by=anti_sample_batches
  )
  #pb = txtProgressBar(min = daysSequence[1], max = daysSequence[length(daysSequence)], initial = daysSequence[1], style=3, label = 'Getting GA Data and sending into BQ')
  for (batchPeriod in seq(from=anti_sample_batches, 
                          to=totalDays, 
                          by=anti_sample_batches)) {
    if (is.null(gaData)) {
      gaData <- google_analytics(
        viewId = view_id,
        date_range = c(endDate - batchPeriod + 1, 
                       endDate - batchPeriod + anti_sample_batches),
        dimensions = dimensions,
        metrics = metrics,
        order = list(
          order_type(order[1], order[2])
        ),
        anti_sample = TRUE,
        slow_fetch = TRUE
      )
      
      send_data_to_bq(
        gaData,
        dataset,
        view_id,
        project_id,
        write_disposition,
        location,
        expiration,
        table = table
      )
      
      
    } else {
      gaData <- google_analytics(
        viewId = view_id,
        date_range = c(endDate - batchPeriod + 1, 
                       endDate - batchPeriod + anti_sample_batches),
        dimensions = dimensions,
        metrics = metrics,
        order = list(
          order_type(order[1], order[2])
        ),
        anti_sample = TRUE,
        slow_fetch = TRUE
      )
      
      send_data_to_bq(
        gaData,
        dataset,
        view_id,
        project_id,
        write_disposition= 'WRITE_APPEND',
        location,
        expiration,
        table = table
      )
      
      
    }
  }
  
  
  
}
