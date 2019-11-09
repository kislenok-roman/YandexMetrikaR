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

setMethod(
  "listRequests",
  "metrikaLogsAPI",
  function(obj) {
    h <- new_handle()
    URL <- paste0("https://api-metrika.yandex.ru/management/v1/counter/", obj@counterID, "/logrequests")
    handle_setheaders(h, "Authorization" = paste0("OAuth ", obj@tokenID))
    res <- curl_fetch_memory(URL, h)

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
