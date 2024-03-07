
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


# 02_collect_rcv ----------------------------------------------------------

get_meetings_api <- function(year){
  httr2::request("https://data.europarl.europa.eu/api/v1/meetings") |> 
    httr2::req_headers(format = "application/ld+json",
                       year = year) |> 
    httr2::req_perform() |> 
    httr2::resp_body_json(simplifyVector = T)
}


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

path <- "01_raw_data/votes/P9_PV(2022)05-05.xml"

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
  
  reds_desc <- description_text |> map(~xml_contents(.x) |> 
                                         xml2::xml_attr("href") |>  
                                         (\(.x).x[!is.na(.x)])()) |> 
    purrr::modify_if(
                                            
                                           ~ length(.) == 0, 
                                           ~ NA_character_
                                         ) |> 
    map(~str_remove_all(.x,".*/"))
  
  
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
  
  
  full <-
    tibble(
      identifier,
      title,
      date,
      tabled_text = table_id,
      tabled_text_href = reds_desc,
      results
    ) |>
    unnest(results) |>
    bind_cols(global) |>
    mutate(path = path) |>
    mutate(across(everything(),as.character))
  
  return(full)
}


# collect rcv from API ----------------------------------------------------

get_meetings_api <- function(year){
  httr2::request("https://data.europarl.europa.eu/api/v1/meetings") |>
    httr2::req_headers(accept = "application/ld+json",
                       year = year) |>
    httr2::req_perform() |>
    httr2::resp_body_json(simplifyVector = T) |>
    purrr::pluck("data")
}

activity_id <- "MTG-PL-2024-01-15"

get_decisions_api <- function(activity_id){
  resp <- httr2::request("https://data.europarl.europa.eu/api/v1/meetings") |>
    httr2::req_url_path_append(activity_id) |>
    httr2::req_url_path_append("decisions") |>
    httr2::req_headers(accept = "application/ld+json") |>
    httr2::req_perform()

  if(httr2::resp_has_body(resp)){
    result <- resp |> 
      httr2::resp_body_json(simplifyVector = T) |>
      purrr::pluck("data") |>
      tidyr::unnest(activity_label) |>
      tidyr::unnest(recorded_in_a_realization_of) |>
      dplyr::select(
        id,
        activity_date,
        activity_id,
        activity_start_date,
        contains("voter"),
        contains("votes"),
        recorded_in_a_realization_of,
        decision_method,
        had_activity_type,
        activity_label_en = en
      ) |>
      tidyr::pivot_longer(cols = contains("voter"), names_to = "vote_type", values_to = "votes") |>
      tidyr::unnest_longer(votes) |>
      dplyr::mutate(intended = dplyr::if_else(stringr::str_detect(vote_type, "intend"), "intended", "actual"),
                    vote_type = stringr::str_remove(vote_type,"had_voter_intended_|had_voter_")) |>
      tidyr::pivot_wider(names_from = "intended", values_from = "vote_type") |>
      dplyr::group_by(id, votes) |>
      dplyr::mutate(n = dplyr::n()) |>
      dplyr::ungroup() |> 
      mutate(across(everything(), as.character)) 
  } else {
    result <- tibble(activity_id, error = "empty body")
  }
  return(result)

}


