source("const.R")

# Классы
setClass(
  "yandexLogsAPI",
  slots = c(
    "counterID" = "integer",
    "tokenID" = "character"
  )
)
setClass("metrikaLogsAPI", contains = "yandexLogsAPI", slots = c("source" = "character"))
setClass("appMetrikaLogsAPI", contains = "yandexLogsAPI")

# Инициализация
initMetrikaLogsAPI <- function(counter, token, source = с("visits", "hits")) {
  source <- match.arg(source)
  new("metrikaLogsAPI", counterID = counter, tokenID = token, source = source)
}
initAppMetrikaLogsAPI <- function(counter, token) {
  new("appMetrikaLogsAPI", counterID = counter, tokenID = token)
}

# Дженерики
setGeneric("listRequests", function(obj) { stop("NA") })
setGeneric("cleanRequest", function(obj, requestID) { stop("NA") })
setGeneric("listFields",   function(obj, filter) { stop("NA") })
setGeneric("makeRequest",  function(obj, from, to, fields) { stop("NA") })

# Методы
setMethod(
  "show",
  "yandexLogsAPI",
  function(object) {
    cat("Yandex Metrika Logs for", object@counterID)
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
      req <- jsonlite::fromJSON(rawToChar(res[["content"]]))[["requests"]]

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
  c("metrikaLogsAPI", "ANY"),
  function(obj, requestID) {
    l <- listRequests(obj)

    if (!is.null(l)) {
      if (missing(requestID)) {
        requestID <- l[, request_id]
      } else {
        requestID <- l[request_id %in% requestID, request_id]
      }

      if (length(requestID) > 0) {
        h <- curl::new_handle()
        curl::handle_setopt(h, copypostfields = "")
        curl::handle_setheaders(h, "Authorization" = paste0("OAuth ", tokenID))
        for (id in requestID) {
          if (l[request_id == id, status] == "processed") {
            action <- "clean"
          } else {
            action <- "cancel"
          }

          URL <- paste0("https://api-metrika.yandex.ru/management/v1/counter/", counterID, "/logrequest/", id , "/", action)
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
    } else if (filrer == "minimal") {
      fields[must == TRUE, field]
    } else {
      fields[stringr::str_detect(field, filter), field]
    }
  }
)

setMethod(
  "makeRequest",
  c("metrikaLogsAPI", "Date", "Date", "character"),
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
      jsonlite::fromJSON(rawToChar(res[["content"]]))[["log_request"]]
    } else {
      warning("Can't access", rawToChar(res[["content"]]))
      NULL
    }
  }
)

metrikaGetResult <- function(counterID, tokenID, requestID) {
  l <- metrikaListRequests(counterID, tokenID)

  if (!is.null(l)) {
    l <- l[request_id == requestID & status == "processed"]
    if (nrow(l) == 1L) {
      h <- new_handle()
      handle_setheaders(h, "Authorization" = paste0("OAuth ", tokenID))

      availableParts <- l[["parts"]][[1]][, "part_number"]
      parts <- vector("list", length(availableParts))

      for (partID in availableParts) {
        URL <- paste0(
          "https://api-metrika.yandex.net/management/v1/counter/", counterID,
          "/logrequest/", requestID,
          "/part/", partID, "/download"
        )

        req <- curl_fetch_memory(URL, h)

        if (req[["status_code"]] == 200L) {
          parts[[partID + 1L]] <- fread(rawToChar(req[["content"]]))
        } else {
          warning("Part ", partID, " could not be fetched")
        }
      }

      parts <- rbindlist(parts)

      setnames(parts, str_remove(names(parts), "ym:(s|pv):"))

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
