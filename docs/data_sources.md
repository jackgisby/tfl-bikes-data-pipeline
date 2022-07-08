Data sources
============

# TfL data

The TfL data contains information on the cycle journeys and docking stations. While the API provides a consistent interface to many datasets, the historical journey data has been saved in multiple different formats that vary across the time course. We load datasets from two sources: i) the locations data, which has information on the cycle docking stations; ii) the journey data, which has information for each cycle rental, including their start and end times and locations. This second dataset forms the backbone of the BigQuery database generated as part of this pipeline. 

## Locations data

## Journey data

The journey data 

The vast majority of the weekly datasets are saved as comma-delimited CSV files. 

# Weather data

- Daily __rainfall__ (mm)
- Daily __minimum temperature__ (degrees Celcius)
- Daily __maximum temperature__ (degrees Celcius)
