shortFields <- function(d) {
  stringr::str_remove(d, "^ym:(pv|s):")
}
