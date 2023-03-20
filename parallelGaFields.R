dimensionsAndMetrics <- list(
  list(
    overlay = list(
      dimensions = c(
        'clientId', 'date', 'channelGrouping', 'socialEngagementType'
      ), 
      metrics = c('sessions')
    )
  ),
  list(
    device = list(
      dimensions = c(
        "clientId", "date",  "deviceCategory", "browser", "browserVersion", 
        "browserSize", "operatingSystem", "operatingSystemVersion"
      ),
      metrics = c("sessions")
    )
  ),
  list(
    eventInfo = list(
      dimensions = c(
        "clientId","date","dateHourMinute","eventCategory","eventAction", 
        "eventLabel"			
      ),
      metrics = c("hits", "totalEvents", "eventValue")
    )
  ),
  list(
    geoNetwork = list(
      dimensions = c(
        "clientId","date","city","country","region","continent","latitude",
        "longitude","countryIsoCode"
      ),
      metrics = c("sessions")
    )
  ),
  list(
    page = list(
      dimensions = c(
        "clientId","date","dateHourMinute","hostname","pagePath","pageTitle",
        "landingPagePath"
      ),
      metrics = c('hits','pageviews','sessions')
    )
  ),
  list(
    totals = list(
      dimensions = c("clientId","date"),
      metrics = c(
        "hits","sessions","pageviews","timeOnPage","bounces","transactions",
        "transactionRevenue","screenviews","timeOnScreen"
      )
    )
  ),
  list(
    product1 = list(
      dimensions = c(
        "clientId","date","transactionId","productSku","productName",
        "productCategoryHierarchy","productCouponCode"
      ),
      metrics = c(
        "itemQuantity","itemRevenue","productListClicks",
        "productListViews","productRefundAmount"
      )
    )
  ),
  list(
    trafficSource = list(
      dimensions = c(
        "clientId","date","referralPath","campaign","sourceMedium","keyword",
        "adContent","campaignCode","socialNetwork"
      ),
      metrics = c("sessions")
    )
  ),
  list(
    product2 = list(
      dimensions = c(
        "clientId","date","transactionId","productSku","productBrand",
        "productVariant","productListName","productListPosition"			
      ),
      metrics = c('itemQuantity')
    )
  ),
  list(
    transaction = list(
      dimensions = c(
        "clientId","date","dateHourMinute","transactionId","affiliation",
        "checkoutOptions","orderCouponCode"
      ),
      metrics = c(
        "transactionRevenue","transactionTax","transactionShipping",
        "itemQuantity"
      )
    )
  ),
  list(
    promotion = list(
      dimensions = c(
        "clientId","date","dateHourMinute","internalPromotionId",
        "internalPromotionName","internalPromotionCreative",
        "internalPromotionPosition"
      ),
      metrics = c("hits","internalPromotionClicks","internalPromotionViews")
    )
  ),
  list(
    refund = list(
      dimensions = c("clientId","date","dateHourMinute"),
      metrics = c("hits","refundAmount","localRefundAmount")
    )
  )
) 
  