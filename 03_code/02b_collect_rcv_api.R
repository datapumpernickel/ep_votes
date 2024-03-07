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

### collect from 2020 onwards

future::plan("multisession", workers = 10)

meetings <- future_map_dfr(2020:2024,get_meetings_api) |> distinct()

decisions <- map_dfr(meetings$activity_id, get_decisions_api, .progress = T)

decisions_clean <- decisions |> 
  mutate(id = str_remove_all(id,".*event/"),
         decision_method = str_remove_all(decision_method, ".*decision-method/"),
         had_activity_type = str_remove_all(had_activity_type, ".*event/"),
         pers_id = str_remove_all(votes,"person/")
         ) |> 
  select(-error, -n, -votes)

duplicates <- decisions |> 
  count(id,votes) |> 
  filter(n>1)

DBI::dbWriteTable(con, "votes_api", decisions, overwrite = T)
