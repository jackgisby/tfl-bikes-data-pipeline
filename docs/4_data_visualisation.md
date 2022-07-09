Data visualisation
==================

The prototype dashboards created for this project were simple examples designed to show the general properties of the data integrated into the BigQuery dataset. Before creating them, a view was created for the BigQuery dataset, as specified in `sql/make_view.sql`. To replicate these dashboards, create a new dashboard in Data Studio with the newly created view as the data source. Filters were added based on the start and ending location names and the date at which each journey ended. 

# Main dashboard

<p align="center">
  <img src="https://github.com/jackgisby/tfl-bikes-data-pipeline/blob/main/assets/bikes_dashboard.png?raw=true" />
</p>

The visualisations based on google maps require latitude and longitude combined into a single column, like so: "lat, long". This was created in the view using `CONCAT`. Both the station map and the barplot in the main dashboard are based on the end station name. So, the dashboard is currently limited by the fact that filtering by start station may not filter these elements as you might expect. Filtering by start station will show all of the points in the map where journeys started from the selected start station, which may not be the behaviour initially expected. When filtering by end location, only the selected end locations are displayed, as shown in the example below.

<p align="center">
  <img src="https://github.com/jackgisby/tfl-bikes-data-pipeline/blob/main/assets/bikes_dashboard_top_four_destinations.png?raw=true" />
</p>

It would likely be better to include two maps and two barplots: one for the starting location and one for the ending location. This is useful, because you may filter by a set of starting/ending locations, view these stations on one map and view the stations that connect to these locations in another. It would also be useful to colour the station map by "number of journeys"; for instance, you could select a particular starting location, and view the number of journeys to each destination. 

These locations data could also be modelled as a network, where nodes are cycle stations and journeys are edges. An analysis considering the most popular cycling locations could inform efforts to create cycle paths, to improve the safety and convenience of cycling. 

# Weather dasboard

The weather dashboard has a similar limitation to the main dashboard, in that the data shown is only for one end of the journey. However, given that most journeys are not very long, the start and ending weather are likely quite similar regardless. Regardless, it demonstrates that the data could be applied to investigate the effect of weather on the number of journeys that take place. For instance, a model could be created that predicts the number of journeys that will take place from a particular station given its location and weather variables. In particular, rainfall would likely be an important predictor of the number of journeys that take place in an area. 

<p align="center">
  <img src="https://github.com/jackgisby/tfl-bikes-data-pipeline/blob/main/assets/weather_integration.png?raw=true" />
</p>

The plots are simple line plots over time. Two data sources were added to the lower line plot, for each of the minimum and maximum temperature. Where multiple locations are selected, the average temperature across all the locations is calculated for the visualisation. 
