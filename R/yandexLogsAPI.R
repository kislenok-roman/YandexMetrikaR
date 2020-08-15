source("R/const.R")
source("R/util.R")

# Классы
setClass(
  "yandexLogsAPI",
  slots = c(
    "counterID" = "integer",
    "tokenID" = "character"
  )
)
setClass("metrikaLogsAPI", contains = "yandexLogsAPI", slots = c("source" = "character"))
setClass("appMetrikaLogsAPI", contains = "yandexLogsAPI", slots = c("source" = "character"))
setClass("metrikaRequest", slots = c("source" = "ANY", "counter" = "metrikaLogsAPI"))

# Инициализация
initMetrikaLogsAPI <- function(counter, token, source = c("visits", "hits")) {
  source <- match.arg(source)
  new("metrikaLogsAPI", counterID = counter, tokenID = token, source = source)
}
initAppMetrikaLogsAPI <- function(counter, token, source = c("events")) {
  source <- match.arg(source)
  new("appMetrikaLogsAPI", counterID = counter, tokenID = token, source = source)
}

# Дженерики
setGeneric("listRequests", function(obj, requestID = NULL) { stop("NA") })
setGeneric("getRequestStatus", function(obj) { stop(NA) })
setGeneric("cleanRequest", function(obj, ...) { stop("NA") })
setGeneric("listFields",   function(obj, filter = NULL) { stop("NA") })
setGeneric("makeRequest",  function(obj, from = NULL, to = NULL, fields = NULL) { stop("NA") })
setGeneric("getResult",    function(obj, ...) { stop("NA") })
setGeneric("refresh",      function(obj) { stop("NA")})

# Методы
setMethod(
  "show",
  "yandexLogsAPI",
  function(object) {
    cat("Yandex Metrika Logs for", object@counterID)
  }
)

setMethod(
  "show",
  "metrikaRequest",
  function(object) {
    if (length(object@source) > 0L) {
      cat(
        "Yandex Request for ", object@source[["source"]], " #", object@source[["request_id"]],
        "\n\tCounterID:\t", object@source[["counter_id"]],
        "\n\tPeriod:   \t "
      )
      if (object@source[["date1"]] == object@source[["date2"]]) {
        cat(object@source[["date1"]])
      } else {
        cat(object@source[["date1"]], "-", object@source[["date2"]])
      }
      cat("\n\tFields:   \t", paste0(shortFields(object@source[["fields"]]), collapse = ", "))
    } else {
      cat("Empty request")
    }
  }
)

setMethod(
  "listRequests",
  "metrikaLogsAPI",
  function(obj, requestID = NULL) {
    h <- curl::new_handle()
    URL <- paste0("https://api-metrika.yandex.ru/management/v1/counter/", obj@counterID, "/logrequests")
    curl::handle_setheaders(h, "Authorization" = paste0("OAuth ", obj@tokenID))
    res <- curl::curl_fetch_memory(URL, h)

    if (res[["status_code"]] == 200L) {
      req <- jsonlite::fromJSON(rawToChar(res[["content"]]))[["requests"]]

      if (length(req) > 0L) {
        if (!missing(requestID) && !is.null(requestID) && !is.na(requestID)) {
          l <- list()
          for (i in 1:nrow(req)) {
            if (req[i, "request_id"] == requestID) {
              l[[1]] <- req[i, ]
              break
            }
          }
          l
        } else {
          req
        }
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
  "getRequestStatus",
  "metrikaRequest",
  function(obj) {
    l <- listRequests(obj@counter, obj@source[["request_id"]])

    if (is.null(l)) {
      "nothing"
    } else {
      l[[1]][["status"]]
    }
  }
)

setMethod(
  "cleanRequest",
  "metrikaLogsAPI",
  function(obj, requestID) {
    l <- listRequests(obj)

    if (!is.null(l)) {
      if (missing(requestID)) {
        requestID <- l[, "request_id"]
      } else {
        requestID <- l[l[, "request_id"] %in% requestID, "request_id"]
      }

      if (length(requestID) > 0) {
        h <- curl::new_handle()
        curl::handle_setopt(h, copypostfields = "")
        curl::handle_setheaders(h, "Authorization" = paste0("OAuth ", obj@tokenID))
        for (id in requestID) {
          if (l[l[, "request_id"] == id, "status"] == "processed") {
            action <- "clean"
          } else {
            action <- "cancel"
          }

          URL <- paste0("https://api-metrika.yandex.ru/management/v1/counter/", obj@counterID, "/logrequest/", id , "/", action)
          res <- curl::curl_fetch_memory(URL, h)

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
  "cleanRequest",
  "metrikaRequest",
  function(obj) {
    cleanRequest(obj@counter, obj@source[["request_id"]])
  }
)

setMethod(
  "listFields",
  "metrikaLogsAPI",
  function(obj, filter = NULL) {
    if (obj@source == "hits") {
      fields <- metrikaLogsAPIHitsFields
    } else {
      fields <- metrikaLogsAPIVisitsFields
    }

    if (is.null(filter) || filter == "all" || is.na(filter) || nchar(filter) == 0) {
      fields[, field]
    } else if (filter == "minimal") {
      fields[must == TRUE, field]
    } else {
      fields[stringr::str_detect(field, filter), field]
    }
  }
)

setMethod(
  "makeRequest",
  c("metrikaLogsAPI"),
  function(obj, from = Sys.Date() - 1, to = Sys.Date() - 1, fields = listFields(obj, "minimal")) {
    prefix <- c("visits" = "ym:s:", "hits" = "ym:pv:")[obj@source]

    fields <- paste0(prefix, fields, collapse = ",")

    URL <- paste0("https://api-metrika.yandex.ru/management/v1/counter/", obj@counterID, "/logrequests")
    POST <- paste0(
      "date1=", from,
      "&date2=", to,
      "&fields=", fields,
      "&source=", obj@source
    )
    h <- curl::new_handle()
    curl::handle_setopt(h, copypostfields = POST)
    curl::handle_setheaders(h, "Authorization" = paste0("OAuth ", obj@tokenID))

    res <- curl::curl_fetch_memory(URL, h)

    if (res[["status_code"]] == 200L) {
      new(
        "metrikaRequest",
        source = jsonlite::fromJSON(
          rawToChar(res[["content"]])
        )[["log_request"]],
        counter = obj
      )
    } else {
      warning("Can't access", rawToChar(res[["content"]]))
      NULL
    }
  }
)

setMethod(
  "getResult",
  "metrikaLogsAPI",
  function(obj, requestID, callback = NULL) {
    l <- listRequests(obj)

    if (!is.null(l)) {
      l <- l[l[, "request_id"] == requestID & l[, "status"] == "processed", ]
      if (nrow(l) == 1L) {
        h <- curl::new_handle()
        curl::handle_setheaders(h, "Authorization" = paste0("OAuth ", obj@tokenID))

        availableParts <- l[["parts"]][[1]][, "part_number"]
        parts <- vector("list", length(availableParts))

        for (partID in availableParts) {
          URL <- paste0(
            "https://api-metrika.yandex.net/management/v1/counter/", obj@counterID,
            "/logrequest/", requestID,
            "/part/", partID, "/download"
          )

          req <- curl::curl_fetch_memory(URL, h)

          if (req[["status_code"]] == 200L) {
            parts[[partID + 1L]] <- data.table::fread(rawToChar(req[["content"]]))
          } else {
            warning("Part ", partID, " could not be fetched")
          }
        }

        parts <- data.table::rbindlist(parts)

        data.table::setnames(parts, stringr::str_remove(names(parts), "ym:(s|pv):"))

        parts
      } else {
        warning("Not ready yet")
        NULL
      }
    } else {
      warning("No requests")
      NULL
    }
  }
)

setMethod(
  "getResult",
  "metrikaRequest",
  function(obj) {
    getResult(obj@counter, obj@source[["request_id"]])
  }
)
