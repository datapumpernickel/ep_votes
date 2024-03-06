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
cd_api <- cachem::cache_disk(dir = "01_raw_data/mep_membership_api", max_size = 1024*1024^2*10)

# get mep list ------------------------------------------------------------
get_mep_list <- memoise(get_mep_list,cache  = cd_api)
meps <- map_dfr(1:9,get_mep_list) |> 
  distinct()

plan("multisession", workers = 32)
get_mep <- memoise(get_mep,cache  = cd_api)
get_org <- memoise(get_org,cache  = cd_api)

meps_info <- future_map(unique(meps$identifier),get_mep,.progress = T) |> 
  future_map_dfr(~select(.x,-any_of(c("hasEmail","homepage"))) |> 
                   unnest(any_of("placeOfBirth")))

meps_ep_membership <- meps_info  |>
  mutate(nationality = str_remove_all(citizenship,".*country/")) |> 
  mutate(gender = str_remove_all(hasGender,".*human-sex/") |> tolower()) |> 
  filter(stringi::stri_detect_fixed(hasMembership_organization,"ep-")) |> 
  filter(str_detect(hasMembership_role,"/MEMBER$")) |> 
  mutate(start_date = ymd_hms(hasMembership_memberDuring_startDate)) |> 
  mutate(end_date = ymd_hms(hasMembership_memberDuring_endDate)) |> 
  select(identifier, type, label, start_date, end_date,nationality,gender,contains(c("Name"))) |> distinct() |> 
  mutate(end_date = if_else(is.na(end_date), Sys.Date(), end_date |> as_date()),
         start_date = as_date(start_date))

meps_party_membership <- meps_info |> 
  filter(stringi::stri_detect_fixed(hasMembership_membershipClassification,"NP")) |> 
  select(identifier, type, label, contains(c("endDate","startDate")),hasMembership_organization) |> 
  mutate(start_date = ymd_hms(hasMembership_memberDuring_startDate)) |> 
  mutate(end_date = ymd_hms(hasMembership_memberDuring_endDate)) |> 
  select(identifier, type, label, start_date, end_date,hasMembership_organization) |> 
  distinct() |> 
  mutate(end_date = if_else(is.na(end_date), Sys.Date(), end_date |> as_date()),
         start_date = as_date(start_date)) |> 
  mutate(identifier_party = str_remove_all(hasMembership_organization, "org/"))

party_names <- meps_party_membership |> 
  distinct(identifier_party) |> 
  pull(1) |> 
  future_map_dfr(get_org,.progress = T)

meps_party_membership <- meps_party_membership |> 
  left_join(party_names |> select(short_party, long_party, identifier_party)) |> 
  select(-hasMembership_organization)

DBI::dbWriteTable(con, "meps_ep_membership", meps_ep_membership, overwrite = T)

DBI::dbWriteTable(con, "meps_party_membership", meps_party_membership, overwrite = T)
