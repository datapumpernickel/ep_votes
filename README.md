# Collect Roll Call Votes of the European Parliament

## Steps needed
1. Collect a nice table with all Members of the European Parliament 👩
2. Collect and parse the Roll Call Sheets of the EP 🗳️
3. Collect additional meta data on each vote 🗃️


### 1. Collect a nice table with all Members of the European Parliament 👩
The central aspect we need is: 
- The names of MEPs
- their party memberships, with possible changes
- the respective MEP ids

I use a three-step process, that makes use of different aspects of the above: 

1. Download the RDF from the open data portal
   - Always get the most up to date files automatically
3. Parse the Open Data Portal RDF
   - Get all the links to info about individual memberships (be it in parties, committees or factions), this gives us e.g. links like this: https://data.europarl.europa.eu/membership/23716-f-109405
5. Scrape this membership link
   - E.g.:https://data.europarl.europa.eu/membership/23716-f-109405
   - MEP with the ID 23716, about any possible memberships in national parties, (concept = "http://publications.europa.eu/resource/authority/corporate-body-classification/NP") and the respective start and end dates for the membership.

This results in a list of ~6000 member of parliament - party membership rows. 

--> See file: [01_collect_mep_data](https://github.com/datapumpernickel/ep_votes/blob/main/03_code/01_collect_mep_data.R) 


There is several other ways to get this info, among them: 
* scrape the webpage of the european parliament, but this does not give us reliable info on past terms
* get the .rdf files from the Open Data Portal: https://data.europarl.europa.eu/en/datasets/members-of-the-european-parliament-meps-parliamentary-term9/38
  * These files contain a lot of info, but the membership start and end dates for each organization are not reliably provided  
* use the API for mep data: https://data.europarl.europa.eu/en/developer-corner/opendata-api
  * Same here, membership start and end dates for parties are not reliably reported

### 2. Collect and parse the Roll Call Sheets of the EP 🗳️

tbd

### 3. Collect additional meta data on each vote 🗃️

tbc
