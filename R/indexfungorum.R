#' Send a query to Index Fungorum, with exponential backoff on retries.
#'
#' @param querytype type of query
#' @param timeout timeout for http request
#' @param maxtries maximum number of tries
#' @param tries number of tries that have already been made
#' @param wait seconds to wait between tries
#' @param ... queries to send
#'
#' @return the query response
#' @export
ixf_do_query <- function(querytype, timeout = 60, maxtries = 10, tries = 0, wait = 1, ...) {
  if (tries > maxtries)
    return(xml2::as_xml_document(list(Error = list(Error = TRUE, ...))))
  queries <- list(...)
  queries <- paste0(names(queries), "=", queries, collapse = "&")

  root <- "http://www.indexfungorum.org/ixfwebservice/fungus.asmx/"
  fullquery <- paste0(root, querytype, "?", queries)

  response <- try(httr::GET(url = fullquery, httr::timeout(timeout)))
  if ("try-error" %in% class(response) || httr::http_error(response)) {
    message("Error; retry ", tries + 1, " after ", wait, " sec.")
    Sys.sleep(wait)
    response <- ixf_do_query(querytype, timeout = timeout,
                             maxtries = maxtries, tries = tries + 1,
                             wait = wait * 2, ...)
  }
  return(response)
}

ixf_format_names <- function(data) {
  names(data) <- gsub("x0020_", "", names(data), fixed = TRUE)
  names(data) <- gsub("x0026_", "and", names(data), fixed = TRUE)
  return(data)
}

ixf_parse_schema <- function(xml) {
  schema <- xml2::xml_find_all(xml, "//xs:element[@type]")
  schema <- stats::setNames(xml2::xml_attr(schema, "type"),
                     xml2::xml_attr(schema, "name"))
  schema <- ixf_format_names(schema)
  return(schema)
}

ixf_format_table <- function(data, schema) {
  data <- ixf_format_names(data)

  # take only the parts of the schema which are in this table.
  schema <- schema[names(schema) %in% names(data)]
  intcols <- names(schema)[schema == "xs:int"]
  for (col in intcols) {
    data[[col]] <- as.integer(data[[col]])
  }

  dtcols <- names(schema)[schema == "xs:dateTime"]
  for (col in dtcols) {
    data[[col]] <- lubridate::parse_date_time2(data[[col]], orders = "YmdHMSz")
  }

  data <- data[ , names(schema)]
}

ixf_parse_XML_table <- function(response, schema = NULL) {
  if (inherits(response, c("response"))) {
    response <- httr::content(response,
                              as = "parsed",
                              type = "text/xml",
                              encoding = "UTF-8")
  }
  if (is.null(schema)) schema = ixf_parse_schema(response)
  response <- xml2::xml_find_first(response, "//NewDataSet")
  response <- xml2::as_list(response)
  response <- purrr::map_df(response, purrr::flatten)
  response <- ixf_format_table(response, schema)
  return(response)
}

ixf_parse_XML_list <- function(response) {
  response <- httr::content(response, as = "text", encoding = "UTF-8")
  response <- strsplit(response, "\r\n", fixed = TRUE)
  response <- unlist(response)
  response <- gsub("<[^>]+>", "", response)
  response <- trimws(response)
  response <- response[response != ""]
  return(response)
}

#' Search Index Fungorum
#'
#' @param SearchText Text to search for. May have length >1, in which case
#' multiple queries will be sent.
#' @param AnywhereInText If \code{FALSE}, \code{SearchText} must be the initial
#' substring. If \code{TRUE}, it can occur anywhere in the field
#' @param MaxNumber The maximum number of records to return
#' @param search_type The type of search to perform; i.e., which field will be searches.  Index Fungorum supports \code{"AuthorSearch"},
#' \code{"EpithetSearch"},
#' and \code{"NameSearch"}
#' @param verbose If \code{TRUE}, print a message for each query sent to Index
#' Fungorum.
#' @param schema for internal use.
#' @param ... additional parameters to pass to \code{\link{ixf_do_query}}.  IN particular, \code{timeout} and \code{maxtries} may be useful.
#'
#' @return a \code{\link[tibble]{tibble}} containing Index Fungorum records.
#' @export
#'
#' @examples
#' ixf_Search("muscaria", search_type = "Epithet")
ixf_Search <- function(SearchText,
                       AnywhereInText = FALSE,
                       MaxNumber = 1000,
                       search_type = c("AuthorSearch",
                                       "EpithetSearch",
                                       "NameSearch"),
                       verbose = FALSE,
                       schema = NULL,
                       ...) {

  search_type <- match.arg(search_type)

  if (length(SearchText) > 1) {
    if (is.null(schema)) {
      first <- ixf_do_query(paste0(search_type, "Ds"),
                            SearchText = SearchText,
                            AnywhereInText = AnywhereInText,
                            MaxNumber = MaxNumber,
                            ...)
      first <- httr::content(first,
                             as = "parsed",
                             type = "text/xml",
                             encoding = "UTF-8")
      schema <- ixf_parse_schema(first)
      first <- ixf_parse_XML_table(first, schema)
      data <- purrr::map_dfr(SearchText,
                             ixf_Search,
                             search_type = search_type,
                             AnywhereInText = AnywhereInText,
                             MaxNumber = MaxNumber,
                             verbose = verbose,
                             schema = schema,
                             ...)
      data <- dplyr::bind_rows(first, data)
    } else {
      data <- purrr::map_dfr(SearchText,
                             ixf_Search,
                             search_type = search_type,
                             AnywhereInText = AnywhereInText,
                             MaxNumber = MaxNumber,
                             verbose = verbose,
                             schema = schema,
                             ...)
    }
  } else {
    if (verbose) cat("NameSearch: SearchText = ", SearchText, "\n", sep = "")
    SearchText <- xml2::url_escape(SearchText)
    if (is.null(schema)) {
      data <- ixf_do_query(paste0(search_type, "Ds"),
                           SearchText = SearchText,
                           AnywhereInText = AnywhereInText,
                           MaxNumber = MaxNumber,
                           ...)
      data <- httr::content(data,
                             as = "parsed",
                             type = "text/xml",
                             encoding = "UTF-8")
      schema <- ixf_parse_schema(data)
    } else {
      data <- ixf_do_query("NameSearch",
                           SearchText = SearchText,
                           AnywhereInText = AnywhereInText,
                           MaxNumber = MaxNumber,
                           ...)
    }
    data <- ixf_parse_XML_table(data)
  }
  return(data)
}

#' Return all Index Fungorum IDs modified since a certain date.
#'
#' The implementation currently use simple regex string processing on the query
#' result, rather than an XML parser, because this seems to be much, much
#' faster.  However, it may be vulnerable to a change in formatting by Index
#' Fungorum.
#'
#' @param startDate The starting date to search for updates. Should be a string
#' in \code{YYYYMMDD[HHMMSS]} format, or a \code{\link{Date}}, \code{\link{POSIXct}}, or
#' \code{\link{POSIXlt}} object.
#' @param verbose If \code{TRUE}, print a message when sending the query and
#' receiving the response.
#'
#' @return A character vector of numeric Index Fungorum record IDs.
#' @export
#'
#' @examples
#' ixf_AllUpdatedNames("20180101")
ixf_AllUpdatedNames <- function(startDate, verbose = FALSE) {
  if (inherits(startDate, c("POSIXct", "POSIXlt", "Date"))) {
    startDate <- format(startDate, "%Y%m%d%H%M%S")
  }
  if (verbose) message("AllUpdatedNames: startDate = ", startDate, "...")
  response <- ixf_do_query("AllUpdatedNames", startDate = startDate)

  if (verbose) message("  * parsing response...")
  response <- ixf_parse_XML_list(response)

  #Responses come in the form "urn:lsid:indexfungorum.org:names:216846"
  #We only need the actual ID.
  response <- sub(".*:", "", response)
  response <- as.integer(response)
  return(response)
}

#' @rdname ixf_Search
#' @export
#' @examples
#' ixf_AuthorSearch("Pers.", FALSE, 10)
ixf_AuthorSearch <- function(SearchText,
                             AnywhereInText = FALSE,
                             MaxNumber = 1000,
                             verbose = FALSE,
                             schema = NULL,
                             ...) {
  ixf_Search(SearchText = SearchText,
             AnywhereInText = AnywhereInText,
             MaxNumber = MaxNumber,
             search_type = "AuthorSearch",
             verbose = verbose,
             schema = schema,
             ...)
}


#' Look up the Index Fungorum record for a taxon.
#'
#' @param NameKey The record number (as \code{integer} or \code{character}) of
#' the taxon. May have length >1, in which case multiple queries will be sent.
#' @param verbose If \code{TRUE}, print a message for each query sent to Index
#' Fungorum.
#' @param schema XML schema.  If the XML schema has already been parsed, it is
#' faster to send it rather than parse it again.
#' @param ... extra arguments passed to \code{\link{ixf_do_query}}
#'
#' @return A \code{\link[tibble]{tibble}} with one row, containing the Index
#' Fungorum record for the taxon.
#' @export
#'
#' @examples
#' ixf_NameByKey(303114)
ixf_NameByKey <- function(NameKey, verbose = FALSE, schema = NULL, ...) {
  if (length(NameKey) > 1) {
    if (is.null(schema)) {
      first <- ixf_do_query("NameByKeyDs",
                            NameKey = NameKey[1],
                            verbose = verbose,...)
      first <- httr::content(first,
                             as = "parsed",
                             type = "text/xml",
                             encoding = "UTF-8")
      schema <- ixf_parse_schema(first)
      first <- ixf_parse_XML_table(first, schema)
      data <- purrr::map_dfr(NameKey[-1], ixf_NameByKey, verbose, schema, ...)
      data <- dplyr::bind_rows(first, data)
    } else {
      data <- purrr::map_dfr(NameKey, ixf_NameByKey, verbose, schema, ...)
    }
  } else {
    if (verbose) cat("NameByKey: NameKey =", NameKey, "\r",
                     file = stderr())
    if (is.null(schema)) {
      data <- ixf_do_query("NameByKeyDs", NameKey = NameKey, ...)
      data <- httr::content(data,
                             as = "parsed",
                             type = "text/xml",
                             encoding = "UTF-8")
      schema <- ixf_parse_schema(data)
    } else {
      data <- ixf_do_query("NameByKey", NameKey = NameKey, ...)
    }
    data <- ixf_parse_XML_table(data, schema)
  }
  return(data)
}

#' Look up all Index Fungorum records which list a given taxon as their current name.
#'
#' @param CurrentKey The record number (as \code{integer} or \code{character}) of
#' the current taxon. May have length >1, in which case multiple queries will be sent.
#' @param verbose If \code{TRUE}, print a message for each query sent to Index
#' Fungorum.
#' @param schema XML schema.  If the XML schema has already been parsed, it is
#' faster to send it rather than parse it again.
#' @param ... extra arguments passed to \code{\link{ixf_do_query}}
#'
#' @return A \code{\link[tibble]{tibble}} with one row for each synonym,
#' containing the Index Fungorum records for the synonyms.
#' @export
#'
#' @examples
#' ixf_NameByCurrentKey(303114)
ixf_NamesByCurrentKey <- function(CurrentKey, verbose = FALSE, schema = NULL, ...) {
  if (length(CurrentKey) > 1) {
    if (is.null(schema)) {
      first <- ixf_do_query("NamesByCurrentKeyDs",
                            CurrentKey = CurrentKey[1],
                            verbose = verbose,...)
      first <- httr::content(first,
                             as = "parsed",
                             type = "text/xml",
                             encoding = "UTF-8")
      schema <- ixf_parse_schema(first)
      first <- ixf_parse_XML_table(first, schema)
      data <- purrr::map_dfr(CurrentKey[-1], ixf_NamesByCurrentKey, verbose = verbose, schema = schema, ...)
      data <- dplyr::bind_rows(first, data)
    } else {
      data <- purrr::map_dfr(
        CurrentKey,
        ixf_NamesByCurrentKey,
        verbose = verbose,
        schema = schema,
        ...
      )
    }
  } else {
    if (verbose) cat("NamesByCurrentKey: CurrentKey =", CurrentKey, "\r",
                     file = stderr())
    if (is.null(schema)) {
      data <- ixf_do_query("NamesByCurrentKeyDs", CurrentKey = CurrentKey, ...)
      data <- httr::content(data,
                            as = "parsed",
                            type = "text/xml",
                            encoding = "UTF-8")
      schema <- ixf_parse_schema(data)
    } else {
      data <- ixf_do_query("NamesByCurrentKey", CurrentKey = CurrentKey, ...)
    }
    data <- ixf_parse_XML_table(data, schema)
  }
  return(data)
}

#' Find the full name of a taxon on Index Fungorum
#'
#' @param NameLsid The record number (as \code{integer} or \code{character}) of
#' the taxon.  May have length >1, in which case multiple queries will be sent.
#' @param verbose If \code{TRUE}, print a message for each query sent to Index
#' Fungorum.
#'
#' @return The fully qualified scientific name of the taxon
#' @export
#'
#' @examples
#' ixf_NameFullByKey(303114)
ixf_NameFullByKey <- function(NameLsid, verbose = FALSE) {
  if (length(NameLsid) > 1) {
    purrr::map_chr(NameLsid, ixf_NameFullByKey, verbose)
  } else {
    if (verbose) cat("NameFullByKey: NameLsid = ", NameLsid, "\n", sep = "")
    response <- ixf_do_query("NameFullByKey", NameLsid = NameLsid)
    xml2::xml_text(httr::content(response))
  }
}

#' @rdname ixf_Search
#' @export
#' @examples
#' ixf_NameSearch("Amanita muscaria")
#'
ixf_NameSearch <- function(SearchText,
                           AnywhereInText = FALSE,
                           MaxNumber = 1000,
                           verbose = FALSE,
                           schema = NULL,
                           ...) {
  ixf_Search(SearchText = SearchText,
             AnywhereInText = AnywhereInText,
             MaxNumber = MaxNumber,
             search_type = "NameSearch",
             verbose = verbose,
             schema = schema,
             ...)
}

#' @rdname ixf_Search
#' @export
#' @examples
#' ixf_EpithetSearch("cervesiae", FALSE, 10)
ixf_EpithetSearch <- function(SearchText,
                             AnywhereInText = FALSE,
                             MaxNumber = 1000,
                             verbose = FALSE,
                             schema = NULL,
                             ...) {
  ixf_Search(SearchText = SearchText,
             AnywhereInText = AnywhereInText,
             MaxNumber = MaxNumber,
             search_type = "EpithetSearch",
             verbose = verbose,
             schema = schema,
             ...)
}

