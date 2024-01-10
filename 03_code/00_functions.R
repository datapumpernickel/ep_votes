
# 01_collect_mep_data -----------------------------------------------------


get_dataset_meps <- function(url){
  mep_data <- request(url) |> 
    req_perform() |> 
    resp_body_json(simplifyVector = T)
  
  
  datasets <- mep_data   |>
    pluck("odpDatasetVersions") |>
    select(id, odpResources) |>
    unnest(odpResources, names_repair = "unique") |>
    select(id...1, odpResourceVersions) |>
    unnest(odpResourceVersions) |>
    select(id...1, odpResourceLangs)  |>
    unnest(odpResourceLangs) |> filter(format == "rdf") |> filter(id == max(id)) |>
    select(url, publicationDate) |>
    mutate(
      cover_to = ifelse(
        length(mep_data$coverDateTo) == 0,
        Sys.Date() |> as.character(),
        as.POSIXct(mep_data$coverDateTo / 1000, origin = "1970-01-01", tz = "UTC") |> as.Date() |> as.character()
      ),
      cover_from = as.POSIXct(
        mep_data$coverDateFrom / 1000,
        origin = "1970-01-01",
        tz = "UTC"
      ) |> as.Date() |> as.character()
    )
  if(!file.exists(str_c("01_raw_data/mep/",str_remove(datasets$url,".*tion/")))){
    request(datasets$url) |> 
      req_perform() |> 
      resp_body_string() |> 
      write_lines(str_c("01_raw_data/mep/",str_remove(datasets$url,".*tion/")))
  }
  
  
  return(datasets)
}


parse_mep_file <- function(path){
  xml <- xml2::read_xml(path)
  
  namespaces <- xml2::xml_ns(xml)
  
  mep_data <- xml2::xml_find_all(xml, "foaf:Person") |>
    purrr::map_dfr(~{
      pers_id <- xml2::xml_find_first(.x, "@rdf:about") |> xml2::xml_text()
      
      nationality <- xml2::xml_attr(xml2::xml_find_first(.x, ".//euvoc:represents", ns = namespaces), "resource")
      
      first_name <- xml2::xml_find_first(.x, ".//foaf:givenName", ns = namespaces) |> xml2::xml_text()
      last_name <- xml2::xml_find_first(.x, ".//foaf:familyName", ns = namespaces) |> xml2::xml_text()
      
      memberships <- xml2::xml_find_all(.x, ".//org:hasMembership/org:Membership", ns = namespaces) |>
        purrr::map_dfr(~{
          membership_id <- xml2::xml_attr(.x, "about")
          classification <- xml2::xml_attr(xml2::xml_find_first(.x, "..//epvoc:membershipClassification", ns = namespaces), "resource")
          organization <- xml2::xml_attr(xml2::xml_find_first(.x, "..//org:organization", ns = namespaces), "resource")
          start_date <- xml2::xml_find_first(.x, "..//dcat:startDate") |> xml2::xml_text()
          end_date <- xml2::xml_find_first(.x, "..//dcat:endDate") |> xml2::xml_text()
          
          tibble::tibble(membership_id, classification, organization, start_date, end_date) 
        })
      
      return(tibble::tibble(pers_id, nationality, first_name, last_name, memberships = list(memberships)))
    }, .progress = TRUE)
  
  return(mep_data |> dplyr::mutate(source_path = path))
}

extract_membership_info <- function(url) {
  # Read the XML content from the URL
  xml_content <- xml2::read_xml(url)
  
  # Define namespaces, if necessary
  namespaces <-  xml2::xml_ns(xml_content)
  
  # Extract the start and end dates
  start_date <-  xml2::xml_find_first(xml_content, ".//dcat:startDate") %>%  xml2::xml_text() %>% lubridate::ymd_hms()
  end_date <-  xml2::xml_find_first(xml_content, ".//dcat:endDate") %>%  xml2::xml_text() %>%  lubridate::ymd_hms()
  
  # Extract the English name of the organization
  org_name_en <- xml2::xml_find_first(xml_content, ".//org:Organization/skos:prefLabel[@xml:lang='en']") %>% xml2::xml_text()
  # Return a tibble with the extracted information
  tibble::tibble(
    start_date = start_date,
    end_date = end_date,
    partei_name = org_name_en
  )
}

get_partei_api <- function(path){
  xml <- xml2::read_xml(path)
  # Extract the short label
  short_label <- xml %>% 
    xml2::xml_find_all("//rdfs:label") %>% 
    xml2::xml_text()
  
  long_label <- xml |> 
    xml2::xml_find_all("//skos:prefLabel[@xml:lang='en']")  %>% 
    xml2::xml_text()
  # Create a tibble
  labels <- tibble::tibble(
    long_label,
    short_label
  )
  return(labels)
}






