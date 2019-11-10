require(curl)
require(jsonlite)

setClass(
  "metrikaLogsAPI",
  slots = c(
    "counterID" = "integer",
    "tokenID" = "character"
  )
)

setGeneric(
  "listRequests",
  function(obj) {
    NULL
  }
)

setGeneric(
  "cleanRequest",
  function(obj, requestID) {
    NULL
  }
)

setGeneric(
  "listFields",
  function(obj) {
    NULL
  }
)

setMethod(
  "listRequests",
  "metrikaLogsAPI",
  function(obj) {
    h <- curl::new_handle()
    URL <- paste0("https://api-metrika.yandex.ru/management/v1/counter/", obj@counterID, "/logrequests")
    curl::handle_setheaders(h, "Authorization" = paste0("OAuth ", obj@tokenID))
    res <- curl::curl_fetch_memory(URL, h)

    if (res[["status_code"]] == 200L) {
      req <- fromJSON(rawToChar(res[["content"]]))[["requests"]]

      if (length(req) > 0L) {
        # TODO: return special class
        req
      } else {
        NULL
      }
    } else {
      warning("Can't access", rawToChar(res[["content"]]))
      NULL
    }
  }
)

setMethod(
  "cleanRequest",
  c("metrikaLogsAPI", "integer"),
  function(obj, requestID) {
    l <- listRequests(obj)

    if (!is.null(l)) {
      if (missing(requestID)) {
        requestID <- l[, request_id]
      } else {
        requestID <- l[request_id == requestID, request_id]
      }

      if (length(requestID) > 0) {
        h <- new_handle()
        handle_setopt(h, copypostfields = "")
        handle_setheaders(h, "Authorization" = paste0("OAuth ", tokenID))
        for (id in requestID) {
          if (l[request_id == id, status] == "processed") {
            action <- "clean"
          } else {
            action <- "cancel"
          }

          URL <- paste0("https://api-metrika.yandex.ru/management/v1/counter/", counterID, "/logrequest/", id , "/", action)
          res <- curl_fetch_memory(URL, h)

          if (res[["status_code"]] != 200L) {
            warning("Request ", id, " can't be deleted")
          }
        }
        TRUE
      } else {
        warning("Requests not found")
      }
    } else {
      warning("Nothing deleted")
      FALSE
    }
  }
)

setMethod(
  "listFields",
  "metrikaLogsAPI",
  function(obj, source = c("visits", "hits"), include = c("all", "minimal")) {
    source = match.arg(source)
    include = match.arg(include)

    rbindlist(list(
      # https://yandex.ru/dev/metrika/doc/api2/logs/fields/hits-docpage/
      list(type = "hits",   field = "watchID",                     must = FALSE),
      list(type = "hits",   field = "counterID",                   must = FALSE),
      list(type = "hits",   field = "date",                        must = FALSE),
      list(type = "hits",   field = "dateTime",                    must = TRUE),
      list(type = "hits",   field = "title",                       must = FALSE),
      list(type = "hits",   field = "URL",                         must = TRUE),
      list(type = "hits",   field = "referer",                     must = TRUE),
      list(type = "hits",   field = "UTMCampaign",                 must = FALSE),
      list(type = "hits",   field = "UTMContent",                  must = FALSE),
      list(type = "hits",   field = "UTMMedium",                   must = FALSE),
      list(type = "hits",   field = "UTMSource",                   must = FALSE),
      list(type = "hits",   field = "UTMTerm",                     must = FALSE),
      list(type = "hits",   field = "browser",                     must = FALSE),
      list(type = "hits",   field = "browserMajorVersion",         must = FALSE),
      list(type = "hits",   field = "browserMinorVersion",         must = FALSE),
      list(type = "hits",   field = "browserCountry",              must = FALSE),
      list(type = "hits",   field = "browserEngine",               must = FALSE),
      list(type = "hits",   field = "browserEngineVersion1",       must = FALSE),
      list(type = "hits",   field = "browserEngineVersion2",       must = FALSE),
      list(type = "hits",   field = "browserEngineVersion3",       must = FALSE),
      list(type = "hits",   field = "browserEngineVersion4",       must = FALSE),
      list(type = "hits",   field = "browserLanguage",             must = FALSE),
      list(type = "hits",   field = "clientTimeZone",              must = FALSE),
      list(type = "hits",   field = "cookieEnabled",               must = FALSE),
      list(type = "hits",   field = "deviceCategory",              must = TRUE),
      list(type = "hits",   field = "flashMajor",                  must = FALSE),
      list(type = "hits",   field = "flashMinor",                  must = FALSE),
      list(type = "hits",   field = "from",                        must = FALSE),
      list(type = "hits",   field = "hasGCLID",                    must = FALSE),
      list(type = "hits",   field = "GCLID",                       must = FALSE),
      list(type = "hits",   field = "ipAddress",                   must = FALSE),
      list(type = "hits",   field = "javascriptEnabled",           must = FALSE),
      list(type = "hits",   field = "mobilePhone",                 must = FALSE),
      list(type = "hits",   field = "mobilePhoneModel",            must = FALSE),
      list(type = "hits",   field = "openstatAd",                  must = FALSE),
      list(type = "hits",   field = "openstatCampaign",            must = FALSE),
      list(type = "hits",   field = "openstatService",             must = FALSE),
      list(type = "hits",   field = "openstatSource",              must = FALSE),
      list(type = "hits",   field = "operatingSystem",             must = FALSE),
      list(type = "hits",   field = "operatingSystemRoot",         must = FALSE),
      list(type = "hits",   field = "physicalScreenHeight",        must = FALSE),
      list(type = "hits",   field = "physicalScreenWidth",         must = FALSE),
      list(type = "hits",   field = "regionCity",                  must = FALSE),
      list(type = "hits",   field = "regionCountry",               must = TRUE),
      list(type = "hits",   field = "regionCityID",                must = FALSE),
      list(type = "hits",   field = "regionCountryID",             must = FALSE),
      list(type = "hits",   field = "screenColors",                must = FALSE),
      list(type = "hits",   field = "screenFormat",                must = FALSE),
      list(type = "hits",   field = "screenHeight",                must = FALSE),
      list(type = "hits",   field = "screenOrientation",           must = FALSE),
      list(type = "hits",   field = "screenWidth",                 must = FALSE),
      list(type = "hits",   field = "windowClientHeight",          must = FALSE),
      list(type = "hits",   field = "windowClientWidth",           must = FALSE),
      list(type = "hits",   field = "params",                      must = TRUE),
      list(type = "hits",   field = "lastTrafficSource",           must = FALSE),
      list(type = "hits",   field = "lastSearchEngine",            must = FALSE),
      list(type = "hits",   field = "lastSearchEngineRoot",        must = FALSE),
      list(type = "hits",   field = "lastAdvEngine",               must = FALSE),
      list(type = "hits",   field = "artificial",                  must = TRUE),
      list(type = "hits",   field = "pageCharset",                 must = FALSE),
      list(type = "hits",   field = "link",                        must = FALSE),
      list(type = "hits",   field = "download",                    must = FALSE),
      list(type = "hits",   field = "notBounce",                   must = TRUE),
      list(type = "hits",   field = "lastSocialNetwork",           must = FALSE),
      list(type = "hits",   field = "httpError",                   must = TRUE),
      list(type = "hits",   field = "clientID",                    must = TRUE),
      list(type = "hits",   field = "networkType",                 must = FALSE),
      list(type = "hits",   field = "lastSocialNetworkProfile",    must = FALSE),
      list(type = "hits",   field = "lastSearchEngineRoot",        must = FALSE),
      list(type = "hits",   field = "goalsID",                     must = TRUE),
      list(type = "hits",   field = "shareService",                must = FALSE),
      list(type = "hits",   field = "shareURL",                    must = FALSE),
      list(type = "hits",   field = "shareTitle",                  must = FALSE),
      list(type = "hits",   field = "iFrame",                      must = FALSE),

      # https://yandex.ru/dev/metrika/doc/api2/logs/fields/visits-docpage/
      list(type = "visits", field = "visitID",                     must = TRUE),
      list(type = "visits", field = "counterID",                   must = FALSE),
      list(type = "visits", field = "watchIDs",                    must = TRUE),
      list(type = "visits", field = "date",                        must = FALSE),
      list(type = "visits", field = "dateTime",                    must = TRUE),
      list(type = "visits", field = "dateTimeUTC",                 must = FALSE),
      list(type = "visits", field = "isNewUser",                   must = TRUE),
      list(type = "visits", field = "startURL",                    must = TRUE),
      list(type = "visits", field = "endURL",                      must = FALSE),
      list(type = "visits", field = "pageViews",                   must = TRUE),
      list(type = "visits", field = "visitDuration",               must = TRUE),
      list(type = "visits", field = "bounce",                      must = TRUE),
      list(type = "visits", field = "ipAddress",                   must = FALSE),
      list(type = "visits", field = "regionCountry",               must = TRUE),
      list(type = "visits", field = "regionCity",                  must = FALSE),
      list(type = "visits", field = "regionCountryID",             must = FALSE),
      list(type = "visits", field = "regionCityID",                must = FALSE),
      list(type = "visits", field = "params",                      must = TRUE),
      list(type = "visits", field = "clientID",                    must = TRUE),
      list(type = "visits", field = "networkType",                 must = FALSE),
      list(type = "visits", field = "goalsID",                     must = TRUE),
      list(type = "visits", field = "goalsSerialNumber",           must = FALSE),
      list(type = "visits", field = "goalsDateTime",               must = TRUE),
      list(type = "visits", field = "goalsPrice",                  must = FALSE),
      list(type = "visits", field = "goalsOrder",                  must = FALSE),
      list(type = "visits", field = "goalsCurrency",               must = FALSE),
      list(type = "visits", field = "lastTrafficSource",           must = FALSE),
      list(type = "visits", field = "lastAdvEngine",               must = FALSE),
      list(type = "visits", field = "lastReferalSource",           must = FALSE),
      list(type = "visits", field = "lastSearchEngineRoot",        must = FALSE),
      list(type = "visits", field = "lastSearchEngine",            must = FALSE),
      list(type = "visits", field = "lastSocialNetwork",           must = FALSE),
      list(type = "visits", field = "lastSocialNetworkProfile",    must = FALSE),
      list(type = "visits", field = "referer",                     must = FALSE),
      list(type = "visits", field = "lastDirectClickOrder",        must = FALSE),
      list(type = "visits", field = "lastDirectBannerGroup",       must = FALSE),
      list(type = "visits", field = "lastDirectClickBanner",       must = FALSE),
      list(type = "visits", field = "lastDirectClickOrderName",    must = FALSE),
      list(type = "visits", field = "lastClickBannerGroupName",    must = FALSE),
      list(type = "visits", field = "lastDirectClickBannerName",   must = FALSE),
      list(type = "visits", field = "lastDirectPhraseOrCond",      must = FALSE),
      list(type = "visits", field = "lastDirectPlatformType",      must = FALSE),
      list(type = "visits", field = "lastDirectPlatform",          must = FALSE),
      list(type = "visits", field = "lastDirectConditionType",     must = FALSE),
      list(type = "visits", field = "lastCurrencyID",              must = FALSE),
      list(type = "visits", field = "from",                        must = FALSE),
      list(type = "visits", field = "UTMCampaign",                 must = FALSE),
      list(type = "visits", field = "UTMContent",                  must = FALSE),
      list(type = "visits", field = "UTMMedium",                   must = FALSE),
      list(type = "visits", field = "UTMSource",                   must = FALSE),
      list(type = "visits", field = "UTMTerm",                     must = FALSE),
      list(type = "visits", field = "openstatAd",                  must = FALSE),
      list(type = "visits", field = "openstatCampaign",            must = FALSE),
      list(type = "visits", field = "openstatService",             must = FALSE),
      list(type = "visits", field = "openstatSource",              must = FALSE),
      list(type = "visits", field = "hasGCLID",                    must = FALSE),
      list(type = "visits", field = "lastGCLID",                   must = FALSE),
      list(type = "visits", field = "firstGCLID",                  must = FALSE),
      list(type = "visits", field = "lastSignificantGCLID",        must = FALSE),
      list(type = "visits", field = "browserLanguage",             must = FALSE),
      list(type = "visits", field = "browserCountry",              must = TRUE),
      list(type = "visits", field = "clientTimeZone",              must = FALSE),
      list(type = "visits", field = "deviceCategory",              must = TRUE),
      list(type = "visits", field = "mobilePhone",                 must = FALSE),
      list(type = "visits", field = "mobilePhoneModel",            must = FALSE),
      list(type = "visits", field = "operatingSystemRoot",         must = FALSE),
      list(type = "visits", field = "operatingSystem",             must = FALSE),
      list(type = "visits", field = "browser",                     must = FALSE),
      list(type = "visits", field = "browserMajorVersion",         must = FALSE),
      list(type = "visits", field = "browserMinorVersion",         must = FALSE),
      list(type = "visits", field = "browserEngine",               must = FALSE),
      list(type = "visits", field = "browserEngineVersion1",       must = FALSE),
      list(type = "visits", field = "browserEngineVersion2",       must = FALSE),
      list(type = "visits", field = "browserEngineVersion3",       must = FALSE),
      list(type = "visits", field = "browserEngineVersion4",       must = FALSE),
      list(type = "visits", field = "cookieEnabled",               must = FALSE),
      list(type = "visits", field = "javascriptEnabled",           must = FALSE),
      list(type = "visits", field = "flashMajor",                  must = FALSE),
      list(type = "visits", field = "flashMinor",                  must = FALSE),
      list(type = "visits", field = "screenFormat",                must = FALSE),
      list(type = "visits", field = "screenColors",                must = FALSE),
      list(type = "visits", field = "screenOrientation",           must = FALSE),
      list(type = "visits", field = "screenWidth",                 must = FALSE),
      list(type = "visits", field = "screenHeight",                must = FALSE),
      list(type = "visits", field = "physicalScreenWidth",         must = FALSE),
      list(type = "visits", field = "physicalScreenHeight",        must = FALSE),
      list(type = "visits", field = "windowClientWidth",           must = FALSE),
      list(type = "visits", field = "windowClientHeight",          must = FALSE),
      list(type = "visits", field = "purchaseID",                  must = FALSE),
      list(type = "visits", field = "purchaseDateTime",            must = FALSE),
      list(type = "visits", field = "purchaseAffiliation",         must = FALSE),
      list(type = "visits", field = "purchaseRevenue",             must = FALSE),
      list(type = "visits", field = "purchaseTax",                 must = FALSE),
      list(type = "visits", field = "purchaseShipping",            must = FALSE),
      list(type = "visits", field = "purchaseCoupon",              must = FALSE),
      list(type = "visits", field = "purchaseCurrency",            must = FALSE),
      list(type = "visits", field = "purchaseProductQuantity",     must = FALSE),
      list(type = "visits", field = "productsPurchaseID",          must = FALSE),
      list(type = "visits", field = "productsID",                  must = FALSE),
      list(type = "visits", field = "productsName",                must = FALSE),
      list(type = "visits", field = "productsBrand",               must = FALSE),
      list(type = "visits", field = "productsCategory",            must = FALSE),
      list(type = "visits", field = "productsCategory1",           must = FALSE),
      list(type = "visits", field = "productsCategory2",           must = FALSE),
      list(type = "visits", field = "productsCategory3",           must = FALSE),
      list(type = "visits", field = "productsCategory4",           must = FALSE),
      list(type = "visits", field = "productsCategory5",           must = FALSE),
      list(type = "visits", field = "productsVariant",             must = FALSE),
      list(type = "visits", field = "productsPosition",            must = FALSE),
      list(type = "visits", field = "productsPrice",               must = FALSE),
      list(type = "visits", field = "productsCurrency",            must = FALSE),
      list(type = "visits", field = "productsCoupon",              must = FALSE),
      list(type = "visits", field = "productsQuantity",            must = FALSE),
      list(type = "visits", field = "impressionsURL",              must = FALSE),
      list(type = "visits", field = "impressionsDateTime",         must = FALSE),
      list(type = "visits", field = "impressionsProductID",        must = FALSE),
      list(type = "visits", field = "impressionsProductName",      must = FALSE),
      list(type = "visits", field = "impressionsProductBrand",     must = FALSE),
      list(type = "visits", field = "impressionsProductCategory",  must = FALSE),
      list(type = "visits", field = "impressionsProductCategory1", must = FALSE),
      list(type = "visits", field = "impressionsProductCategory2", must = FALSE),
      list(type = "visits", field = "impressionsProductCategory3", must = FALSE),
      list(type = "visits", field = "impressionsProductCategory4", must = FALSE),
      list(type = "visits", field = "impressionsProductCategory5", must = FALSE),
      list(type = "visits", field = "impressionsProductVariant",   must = FALSE),
      list(type = "visits", field = "impressionsProductPrice",     must = FALSE),
      list(type = "visits", field = "impressionsProductCurrency",  must = FALSE),
      list(type = "visits", field = "impressionsProductCoupon",    must = FALSE),
      list(type = "visits", field = "offlineCallTalkDuration",     must = FALSE),
      list(type = "visits", field = "offlineCallHoldDuration",     must = FALSE),
      list(type = "visits", field = "offlineCallMissed",           must = FALSE),
      list(type = "visits", field = "offlineCallTag",              must = FALSE),
      list(type = "visits", field = "offlineCallFirstTimeCaller",  must = FALSE),
      list(type = "visits", field = "offlineCallURL",              must = FALSE)
    ))[type == source & (include == "all" | must == TRUE)][, field]
  }
)


