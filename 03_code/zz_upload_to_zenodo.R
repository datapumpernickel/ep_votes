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
    "zen4R",
    "httr2"
  )

p_load(char = packs)


access_token <- Sys.getenv("zenodo_token")

# Get the list of depositions
dep_list <- request("https://zenodo.org/api/deposit/depositions") |>
  req_url_query(access_token = access_token,
                q = "European Roll") |>
  req_perform() |> 
  resp_body_json(simplifyVector = T)


## get detailed bucket list or file upload
dep_id  <- request(str_c("https://zenodo.org/api/deposit/depositions/",
                         dep_list |> pluck("id")
                         )) |>
  req_url_query(access_token = access_token) |>
  req_perform() |> 
  resp_body_json(simplifyVector = T)

# make metadata -- only necessary the very very first time
# resp <- request(str_c("https://zenodo.org/api/deposit/depositions/",
#                       dep_list |> pluck("id"))) |>
#   req_method("PUT") |> 
#   req_url_query(access_token = access_token) |>
#   req_body_json(data =
#                   list(
#                     metadata = list(
#                       title = "European Roll Call Votes",
#                       upload_type = "dataset",
#                       creators = list(
#                         list(
#                           name = "Bochtler, Paul",
#                           affiliation = "SWP",
#                           orcid = "0000-0002-9146-6185"
#                         )
#                       )
#                     )
#                   )) |>
#   req_perform() 

## new version

resp <- request(dep_id |> pluck("links","newversion"))  |>
  req_method("POST") |> 
  req_url_query(access_token = access_token) |> 
  req_perform()

dep_id <- resp |> 
  resp_body_json(simplifyVector = T)

resp <- request(dep_id |> pluck("links","latest_draft"))  |>
  req_url_query(access_token = access_token) |> 
  req_perform()

dep_id <- resp |> 
  resp_body_json(simplifyVector = T)

for(file in dep_id |> pluck("files") |> pull(id)){
  request(str_c(dep_id |> pluck("links","files"), "/",file))  |>
    req_method("DELETE") |> 
    req_url_query(access_token = access_token) |> 
    req_perform()
}



bucket <- resp |> 
  resp_body_json() |> 
  pluck("links") |> 
  pluck("bucket")

id <- resp |> 
  resp_body_json() |> 
  pluck("id")

request(str_c(bucket,"/covoting_table.rds")) |>
  req_method("PUT") |> 
  req_url_query(access_token = access_token) |> 
  req_body_file(path = "04_clean_data/covoting_table.rds") |> 
  # req_dry_run()
  req_perform() 

request(str_c(bucket,"/ep_votes_data.sqlite")) |>
  req_method("PUT") |> 
  req_url_query(access_token = access_token) |> 
  req_body_file(path = "04_clean_data/ep_votes_data.sqlite") |> 
  # req_dry_run()
  req_perform() 

resp <- request(str_c("https://zenodo.org/api/deposit/depositions/",
                      dep_id |> pluck("id"))) |>
  req_method("PUT") |> 
  req_url_query(access_token = access_token) |>
  req_body_json(data =
                  list(
                    metadata = list(
                      title = "European Roll Call Votes",
                      publication_date = Sys.Date(),
                      upload_type = "dataset",
                      creators = list(
                        list(
                          name = "Bochtler, Paul",
                          affiliation = "SWP",
                          orcid = "0000-0002-9146-6185"
                        )
                      )
                    )
                  )) |>
  req_perform() 

 
request(dep_id |> pluck("links","publish"))  |>
  req_method("POST") |> 
  req_url_query(access_token = access_token) |> 
  req_perform()



