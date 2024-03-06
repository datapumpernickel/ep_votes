
# 01_collect_mep_data -----------------------------------------------------

get_mep_list <- function(term){
  url <- "https://data.europarl.europa.eu/api/v1/meps?parliamentary-term={term}&format=application%2Fld%2Bjson&offset=0"
  resp <- request(glue::glue(url)) |> 
    req_perform() |> 
    resp_body_json() |> 
    pluck("data") |> 
    map_dfr(as_tibble)
}

get_mep <- function(id){
  url <- "https://data.europarl.europa.eu/api/v1/meps/{id}?format=application%2Fld%2Bjson&json-layout=framed"
  resp <- httr2::request(glue::glue(url)) |> 
    httr2::req_perform() |> 
    httr2::resp_body_json(simplifyVector = T) |> 
    purrr::pluck("data")  |> 
    tidyr::unnest(hasMembership, names_sep ="_") |> 
    tidyr::unnest(hasMembership_memberDuring, names_sep ="_")
}

get_org <- function(id){
  url <- "https://data.europarl.europa.eu/api/v1/corporate-bodies/{id}?format=application%2Fld%2Bjson&json-layout=framed&language=en"
  resp <- httr2::request(glue::glue(url)) |> 
    httr2::req_perform() |> 
    httr2::resp_body_json(simplifyVector = T) |> 
    purrr::pluck("data")  |> 
    tidyr::unnest(altLabel, names_sep ="_") |> 
    tidyr::unnest(prefLabel, names_sep ="_") |> 
    dplyr::select(hasMembership_organization = id, identifier_party = identifier, short_party = label , long_party =  prefLabel_en)
  return(resp)
}

get_dataset_meps <- function(url) {
  mep_data <- httr2::request(url) |>
    httr2::req_headers(name = "Paul Bochtler",
                       purpose = "scientific research",
                       website = "swp-berlin.org") |>
    httr2::req_perform() |>
    httr2::resp_body_json(simplifyVector = T)
  
  
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




# 02_collect_rcv ----------------------------------------------------------


get_votes <- function(url, dir, id) {
  if (!file.exists(glue('{dir}/{id}.xml'))) {
    res <- request(url) |>
      req_headers(name = "Paul Bochtler", 
                  purpose = "scientific research", 
                  website = "swp-berlin.org") |> 
      req_throttle(30 / 60) |>
      req_retry(max_tries = 20) |>
      req_perform() |>
      resp_body_string() |>
      write_lines(glue('{dir}/{id}.xml'))
  }
}


parse_votes <- function(path) {
  xml_data <- read_xml(path)
  
  global <- tibble(
    sitting_id = xml2::xml_find_first(xml_data, "@Sitting.Identifier") |> xml2::xml_text(),
    sitting_date = xml2::xml_find_first(xml_data, "@Sitting.Date") |> xml2::xml_text(),
    ep_ref = xml2::xml_find_first(xml_data, "@EP.Reference") |> xml2::xml_text(),
    ep_num = xml2::xml_find_first(xml_data, "@EP.Number") |> xml2::xml_text(),
  )
  
  result <- xml_data |> xml2::xml_find_all("//RollCallVote.Result") 
  
  
  identifier <- result |>
    map_chr( ~ xml2::xml_find_first(.x, "@Identifier") |>
               xml2::xml_text())
  
  date <- result |>
    map_chr( ~ xml2::xml_find_first(.x, "@Date") |>
               xml2::xml_text())
  
  title <- result |>
    map_chr( ~ xml2::xml_find_first(.x, "RollCallVote.Description.Text") |>
               xml2::xml_text())  
  
  description_text <- xml_data |> xml2::xml_find_all("//RollCallVote.Description.Text")
  
  table_id <- description_text |>
    map( ~.x |> xml_contents() |> map(~.x |> xml2::xml_text())) |> 
    map_vec( ~ifelse(is.null(.x), NA_character_, .x))
  
  reds_desc <- description_text |> map(~xml_contents(.x) |> xml2::xml_attr("href"))
  
  
  results <- result |>
    map( ~ {
      yes <- xml2::xml_find_all(.x, "Result.For") |>
        map_dfr( ~ {
          
          pol_group_list <- xml2::xml_find_all(.x, "Result.PoliticalGroup.List") 
          political_groups <-pol_group_list |>
            xml2::xml_find_first("@Identifier") |>
            xml2::xml_text()
          meps <- pol_group_list |>
            map2(.x = _, .y = political_groups,
                 ~ {
                   group_member_name <-  xml2::xml_find_all(.x, ".//Member.Name | .//PoliticalGroup.Member.Name")
                   name <- group_member_name|>
                     xml2::xml_text()
                   id <-
                     group_member_name |>
                     xml2::xml_find_first("@MepId")  |>
                     xml2::xml_text()
                   pers_id <-
                     group_member_name |>
                     xml2::xml_find_first("@PersId")  |>
                     xml2::xml_text()
                   
                   data <-
                     tibble(name, id, pers_id) |> mutate(political_group = .y) |>
                     mutate(position = "y")
                 })
          return(meps)
        })
      abstention <- xml2::xml_find_all(.x, "Result.Abstention") |>
        map_dfr( ~ {
          
          pol_group_list <- xml2::xml_find_all(.x, "Result.PoliticalGroup.List") 
          political_groups <-pol_group_list |>
            xml2::xml_find_first("@Identifier") |>
            xml2::xml_text()
          meps <- pol_group_list |>
            map2(.x = _, .y = political_groups,
                 ~ {
                   group_member_name <-  xml2::xml_find_all(.x, ".//Member.Name | .//PoliticalGroup.Member.Name")
                   name <- group_member_name|>
                     xml2::xml_text()
                   id <-
                     group_member_name |>
                     xml2::xml_find_first("@MepId")  |>
                     xml2::xml_text()
                   pers_id <-
                     group_member_name |>
                     xml2::xml_find_first("@PersId")  |>
                     xml2::xml_text()
                   
                   data <-
                     tibble(name, id, pers_id) |> mutate(political_group = .y) |>
                     mutate(position = "a")
                 })
          return(meps)
        })
      against <- xml2::xml_find_all(.x, "Result.Against") |>
        map_dfr( ~ {
          
          pol_group_list <- xml2::xml_find_all(.x, "Result.PoliticalGroup.List") 
          political_groups <-pol_group_list |>
            xml2::xml_find_first("@Identifier") |>
            xml2::xml_text()
          meps <- pol_group_list |>
            map2(.x = _, .y = political_groups,
                 ~ {
                   group_member_name <-  xml2::xml_find_all(.x, ".//Member.Name | .//PoliticalGroup.Member.Name")
                   name <- group_member_name|>
                     xml2::xml_text()
                   id <-
                     group_member_name |>
                     xml2::xml_find_first("@MepId")  |>
                     xml2::xml_text()
                   pers_id <-
                     group_member_name |>
                     xml2::xml_find_first("@PersId")  |>
                     xml2::xml_text()
                   
                   data <-
                     tibble(name, id, pers_id) |> mutate(political_group = .y) |>
                     mutate(position = "n")
                 })
          return(meps)
        })
      
      votes <- bind_rows(yes, against, abstention)
      
    })
  
  
  full <- tibble(identifier, title, date,tabled_text = table_id, results) |>
    unnest(results) |>
    bind_cols(global) |>
    mutate(path = path) |>
    mutate(across(everything(),as.character))
  
  return(full)
}

