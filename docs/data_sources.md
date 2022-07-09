Data sources
============

# TfL data

The TfL data contains information on the cycle journeys and docking stations. While the API provides a consistent interface to many datasets, the historical journey data has been saved in multiple different formats that vary across the time course. We load datasets from two sources: i) the locations data, which has information on the cycle docking stations; ii) the journey data, which has information for each cycle rental, including their start and end times and locations. This second dataset forms the backbone of the BigQuery database generated as part of this pipeline. 

## Locations data

The locations data is [stored as an XML](https://tfl.gov.uk/tfl/syndication/feeds/cycle-hire/livecyclehireupdates.xml). The root node is named "stations", and each child node refers to one location/station at which bikes can be docked. The station nodes contain a number of variables, including:

Variable Name | Data Type | Description
| :---: | :---: | :---:
id | Integer | Unique identifier for the station, used to join the locations data with the journeys data
name | String | The name of the cycle station
terminalName | Integer | A second identifier for the station
lat | Decimal | The latitude of the station
long | Decimal | The longitude of the station
installDate | Integer | The date at which the station was installed
temporary | Boolean | Whether or not this is a temporary station
nbBikes | Integer | The number of bikes currently located at this station
nbEmptyDocks | Integer | How many more bikes the station has capacity for
nbDocks | Integer | The total capacity of the station

Note that we do not use the installDate, temporary, nbBikes, nbEmptyDocks or nbDocks columns. The first two of these columns are not relevant to our analysis. The "nb" columns are subject to constant change, since this XML contains live updates. We are only using the data source to extract the static locations data, so these variables are ignored.

## Journey data

Each row in the journey/usage dataset represents a journey made using a bike, from one destination to another. This dataset forms the basis of the main table in the database created by this pipeline. The vast majority of [the weekly datasets](https://cycling.data.tfl.gov.uk/) are saved as comma-delimited CSV files, although some are saved as XLSX files. The journey data has the following structure:

Column Name | Data Type | Description
| :---: | :---: | :---:
Rental Id | Integer | A unique identifier for the journey
Duration | Integer | The duration of the journey
Bike Id | Integer | A unique identifier for the bike
End Date | Timestamp | The time at which the journey ended
EndStation Id | Integer | A unique identifier for the destination station, relates to the locations data ID
EndStation Name | String | The name of the destination station
Start Date | Timestamp | The time at which the journey began
StartStation Id | Integer | A unique identifier for the origin station, relates to the locations data ID
StartStation Name | String | The name of the origin station

# Weather data

Each row in the weather dataset represents a weather observation for a particular day in a particular location. These datasets contain observations for each point in a 1km by 1km grid across the UK. The data is [stored in netCDF format](https://catalogue.ceda.ac.uk/uuid/4dc8450d889a491ebb20e724debe2dfb), which contains tables describing the latitude, longitude and time of each data point. 

The weather variables we use are as follows:
- Daily __rainfall__ (mm)
- Daily __minimum temperature__ (degrees Celcius)
- Daily __maximum temperature__ (degrees Celcius)
