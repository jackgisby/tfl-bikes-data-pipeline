-- Creates a BigQuery view that joins the locations and weather 
-- data to the fact table by their respective IDs.
CREATE VIEW bikes_data_warehouse.fact_locations_weather_joined AS (
  SELECT 
    fact.rental_id,
    fact.start_station_id,
    fact.end_station_id,
    fact.start_timestamp_id,
    fact.end_timestamp_id,
    fact.start_weather_id,
    fact.end_weather_id,
    start_loc.name AS start_loc_name,
    CONCAT(start_loc.lat, ",", start_loc.long) AS start_lag_long,
    start_weather.rainfall AS start_rainfall,
    start_weather.tasmin AS start_tasmin,
    start_weather.tasmax AS start_tasmax,
    end_loc.name AS end_loc_name,
    CONCAT(end_loc.lat, ",", end_loc.long) AS end_lag_long,
    end_weather.rainfall AS end_rainfall,
    end_weather.tasmin AS end_tasmin,
    end_weather.tasmax AS end_tasmax
  FROM `bikes_data_warehouse.fact_journey` AS fact
  LEFT JOIN `bikes_data_warehouse.dim_locations` AS start_loc
    ON fact.start_station_id = start_loc.id
  LEFT JOIN `bikes_data_warehouse.dim_locations` AS end_loc
    ON fact.end_station_id = end_loc.id
  LEFT JOIN `bikes_data_warehouse.dim_weather` AS start_weather
    ON fact.start_weather_id = start_weather.id
  LEFT JOIN `bikes_data_warehouse.dim_weather` AS end_weather
    ON fact.end_weather_id = end_weather.id
  ORDER BY fact.end_timestamp_id
);
