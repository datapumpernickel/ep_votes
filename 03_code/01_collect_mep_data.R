#### PAUL BOCHTLER
#### 26.10.2023

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
    "dbplyr",
    "memoise",
    "cachem"
  )

p_load(char = packs)

## load source functions for parsing
source("03_code/00_functions.R")

## connect to sqlite database
con <- DBI::dbConnect(RSQLite::SQLite(),"04_clean_data/ep_votes_data.sqlite")
cd <- cachem::cache_disk(dir = "01_raw_data/mep_membership_info", max_size = 1024*1024^2*10)


## get MEP data 
base_url <- "https://data.europarl.europa.eu/OdpDatasetService/Datasets/members-of-the-european-parliament-meps-parliamentary-term"

dir.create("01_raw_data/mep")

datasets_mep <- map_dfr(str_c(base_url,5:9), get_dataset_meps)

plan("multisession", workers = 10)

files <- list.files("01_raw_data/mep",full.names = T)

parse_mep_file <- memoise::memoise(parse_mep_file,cache = cd)

mep_data_complete <- future_map(files, parse_mep_file, .progress = T)

extract_membership_info <- memoise::memoise(extract_membership_info,cache = cd)

extract_membership_info <- safely(extract_membership_info,otherwise = tibble(error = "http_error"))

plan("multisession", workers = 32)

mep_data <- mep_data_complete |> 
  map_dfr(as_tibble) |> 
  ## unnest nested info on parties
  unnest(memberships,keep_empty = T) |>
  ## filter to memberships that are with link to national parties
  filter(stringi::stri_detect_fixed(classification,"/NP"))|> 
  distinct(pers_id, nationality, first_name, last_name, membership_id, organization)|> 
  ## parse membership info from EU rdf sheets
  mutate(extra_info = future_map(membership_id, extract_membership_info, .progress = T)) |> 
  unnest(extra_info) |> 
  unnest(extra_info) |> 
  select(-partei_name) |> 
  mutate(pers_id = str_remove_all(pers_id, ".*/")) |> 
  mutate(nationality = str_remove_all(nationality, ".*/"))|> 
  mutate(full_name = str_c(first_name, " ", last_name)) |> 
  mutate(end_date = if_else(is.na(end_date), Sys.Date(), end_date+1))

get_partei_api <- memoise::memoise(get_partei_api, cache = cd)

## get party names from the EU 
partei_names <- mep_data |> 
  distinct(organization) |>
  filter(!is.na(organization)) |>
  mutate(partei_name = future_map_dfr(organization, get_partei_api))|>
  unnest(partei_name) |> 
  rename(short_party = short_label,
         long_party = long_label)

## merge party names
mep_data <- mep_data |> 
  left_join(partei_names) |> 
  mutate(across(where(is.POSIXct), ~as.Date(.x) |> as.character()))

DBI::dbWriteTable(con, "meps_partei_table", mep_data, overwrite = T)
