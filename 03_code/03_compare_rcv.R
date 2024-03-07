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


votes_api <-  tbl(con, "votes_api") |> collect()

votes_xml <-  tbl(con, "votes_xml") |> collect()

identifier_comparison <- votes_api |> 
  transmute(id = str_extract(id,"\\d+$"),
            activity_date, 
            source = "api") |> distinct() |> 
  full_join(votes_xml |> transmute(id = identifier, sitting_date, source = "xml") |> distinct(), by = "id")
