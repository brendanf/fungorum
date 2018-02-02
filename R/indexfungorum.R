ixf_do_query <- function(querytype, ...) {
  queries <- list(...)
  queries <- paste0(names(queries), "=", queries, collapse = "&")

  root <- "http://www.indexfungorum.org/ixfwebservice/fungus.asmx/"
  fullquery <- paste0(root, querytype, "?", queries)
  message(fullquery)

  response <- httr::GET(url = fullquery)
  response <- httr::content(response,
                            as = "parsed",
                            type = "text/xml",
                            encoding = "utf8")
  return(response)
}

ixf_parse_XML_table <- function(response) {
  response <- xml2::as_list(response)
  response <- lapply(response, purrr::compact)
  response <- lapply(response, dplyr::as_tibble)
  response <- dplyr::bind_rows(response)
  response <- purrr::modify(response, unlist)
  names(response) <- gsub(pattern = "_x0020", replacement = "",
                          x = names(response))
  names(response) <- gsub(pattern = "x0026", replacement = "and",
                          x = names(response))
  return(response)
}

ixf_parse_XML_list <- function(response) {
  response <- xml2::as_list(response)
  response <- unlist(response)
  response <- unname(response)
  return(response)
}

#' Return all Index Fungorum IDs modified since a certain date
#'
#' @param startDate The starting date to search for updates. Should be a string
#' in YYYYMMDD format.
#'
#' @return A character vector of numeric Index Fungorum record IDs.
#' @export
#'
#' @examples
#' ixf_AllUpdatedNames("20180101")
ixf_AllUpdatedNames <- function(startDate) {
  response <- ixf_do_query("AllUpdatedNames", startDate = startDate)
  response <- ixf_parse_XML_list(response)

  #Responses come in the form "urn:lsid:indexfungorum.org:names:216846"
  #We only need the actual ID.
  response <- sub(".*:", "", response)
  return(response)
}

#' Search for Index Fungorum records by the author name
#'
#' @param SearchText The (partial) name of the author to search for
#' @param AnywhereInText If \code{FALSE}, then \code{SearchText} must appear at
#' the beginning of the name record in order to match. If \code{TRUE}, then
#' \{SearchText} can appear anywhere in the field.
#' @param MaxNumber The maximum number of results to return.
#'
#' @return A \code{\link[dplyr]{tibble}} containing the Index Fungorum records
#' which match the author.
#' @export
#'
#' @examples
#' ixf_AuthorSearch("Pers.", FALSE, 10)
ixf_AuthorSearch <- function(SearchText, AnywhereInText, MaxNumber) {
  response <- ixf_do_query("AuthorSearch",
                           SearchText = SearchText,
                           AnywhereInText = AnywhereInText,
                           MaxNumber = MaxNumber)
  ixf_parse_XML_table(response)
}

ixf_NameByKey <- function(NameKey) {
  response <- ixf_do_query("NameByKey", NameKey = NameKey)
  ixf_parse_XML_table(response)
}

ixf_NameFullByKey <- function(NameLsid) {
  response <- ixf_do_query("NameFullByKey", NameLsid = NameLsid)
  xml2::xml_text(response)
}
