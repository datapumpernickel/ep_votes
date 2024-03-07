#### PAUL BOCHTLER

#-----------------------------------------#
#### SET ENVIRONMENT                   ####
#-----------------------------------------#

setwd(dirname(rstudioapi::getActiveDocumentContext()$path))

setwd("../")
getwd()





## empty potential rests from other scripts
rm(list = ls())

## load packages and install missing packages
require("pacman")
## define libraries to be loaded
packs <-
  c(
    'tidyverse',
    "janitor",
    'purrr',
    'rvest',
    'httr2',
    "lubridate",
    'glue',
    "cli",
    'furrr',
    "archive",
    "xml2",
    "DBI",
    "dbplyr"
  )

p_load(char = packs)

source("03_code/00_functions.R")
con <- DBI::dbConnect(RSQLite::SQLite(),"04_clean_data/ep_votes_data.sqlite")

# Get List of Votes from Public Register ----------------------------------


## define request to search public register

body <- '{"dateCriteria":{"field":"DATE_DOCU","startDate":null,"endDate":null},"years":[],"terms":[],"accesses":[],"types":["PPVD"],"authorCodes":[],"fragments":[],"geographicalAreas":[],"eurovocs":[],"directoryCodes":[],"subjectHeadings":[],"policyAreas":[],"institutions":[],"authorAuthorities":[],"recipientAuthorities":[],"authorOrRecipientAuthorities":[],"nbRows":2000,"references":[],"relations":[],"authors":[],"currentPage":1,"sortAndOrder":"DATE_DOCU_DESC","excludesEmptyReferences":false,"fulltext":null,"title":null,"summary":null}'

## execute request
resp <- request("https://www.europarl.europa.eu/RegistreWeb/services/search") |> 
  req_headers("Content-Type" = "application/json", 
              name = "Paul Bochtler", 
              purpose = "scientific research", 
              website = "swp-berlin.org") |> 
  httr2::req_body_raw(body) |> 
  req_perform() |> 
  resp_body_json(simplifyVector = T)

## parse response for needed RCV files
list_of_votes <- resp$references |> 
  select(docId, reference, fragments) |> 
  unnest(fragments) |> 
  unnest(versions) |> 
  unnest(fileInfos)|> 
  filter(value =="RCV") |> 
  filter(typeDoc =="text/xml") |> 
  clean_names() |> 
  select(doc_id, reference, value, title, url, language) |> 
  group_by(reference) |> 
  mutate(duplicates = n()) |> 
  filter(!(duplicates>1 & !language=="FR"))


# Download RCV files ------------------------------------------------------

## is reference a unique identifier? 
## yes it is
length(unique(list_of_votes$reference)) == nrow(list_of_votes)

## make id and links
votes_links <- list_of_votes |> ungroup() |>
  mutate(id = reference) |>
  select(id, url)

## Set Directory for Debate Data
dir <- '01_raw_data/votes'
dir.create(dir, recursive = T)  # Ensure directory exists

plan("multisession", workers = 12)

## Download and Process votes in Parallel
empty_placeholder_for_warnings <- future_pmap(
  list(
    url = votes_links$url,
    dir = dir,
    id = votes_links$id
  ),
  get_votes,
  .progress = T
)



# parse RCV files ---------------------------------------------------------

if(parse){
  ## List Debate File Paths
  files <- list.files(dir, full.names = T)
  
  ## Parse Votes in Parallel
  future::plan("multisession", workers = 32)
  
  ## this takes well over an hour
  full_data <-
    future_map_dfr(files, parse_votes, .progress = T)
  
  write_rds(full_data, "04_clean_data/covoting_table.rds")
}

full_data <- read_rds("04_clean_data/covoting_table.rds")



# missing analysis --------------------------------------------------------


sum(is.na(full_data$pers_id))

length(unique(full_data$name))

distinct_names_per_vote <- full_data |> 
  select(identifier, name) |> 
  count(identifier, name)

missing_identifiers <- full_data |> 
  filter(is.na(identifier)) |> 
  count(path, sitting_id, sitting_date, ep_ref, tabled_text, date, name, title)


library(data.table)

# Assuming full_data is your data frame
setDT(full_data)  # Convert your data frame to a data.table

# Perform the count operation grouped by 'identifier' and 'name'
distinct_names_per_vote <- full_data[, .N, by = .(identifier, name)]

library(data.table)
setDT(positions)  # Convert your data frame to a data.table
duplicates <- positions[, .N, by = .(identifier, pers_id)]

test <- duplicates |>
  dplyr::filter(N>1)

test_2 <- positions |>
  dplyr::filter(identifier =="125101")



# write to sqlite ---------------------------------------------------------

positions <- full_data |>
  dplyr::mutate(sitting_date = lubridate::ymd(sitting_date) ) |>
  dplyr::filter(sitting_date >= lubridate::ymd("2020-01-01")) 




#### write to ssqlite
DBI::dbWriteTable(
  con,
  "votes_xml",
  positions |> 
    distinct(identifier, title, date, sitting_date, sitting_id, ep_ref, ep_num, tabled_text, tabled_text_href),
  overwrite = T
)

DBI::dbWriteTable(
  con,
  "votes_xml_positions",
  positions |>
    distinct(id = identifier, pers_id,name, position),
  overwrite = T
)

test <- tbl(con,"votes_xml_positions") |> collect()


## upload data to zenodo
## clean code to query zenodo 
## get a unique identifier for each individual vote
## get meps with missing pers_id 
## exclude meps that are double in votes by name only (and have missing pers_ids)
## merge meps with missing pers_ids to my data of meps to find their pers_ids
## merge votes and meps 