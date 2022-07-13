-- The 10 most popular bikes - up to 5581 journeys!
SELECT r.bike_id, COUNT(*) AS num_rentals
FROM `bikes_data_warehouse.dim_rental` AS r
GROUP BY r.bike_id ORDER BY num_rentals DESC
LIMIT 10;

-- The 10 most popular destinations in 2021 - up to 85,366 visits (for Hyde Park Corner)!
SELECT l.name, COUNT(*) AS num_visits
FROM `bikes_data_warehouse.fact_journey` AS f
LEFT JOIN `bikes_data_warehouse.dim_locations` AS l
  ON f.end_station_id = l.id
WHERE DATE(f.end_timestamp) >= "2021-01-01" AND DATE(f.end_timestamp) <= "2021-12-31"
GROUP BY l.name ORDER BY num_visits DESC
LIMIT 10;


-- But what was the most popular destination between 3-4AM? Jubilee Gardens, with 372 visits
SELECT l.name, COUNT(*) AS num_visits
FROM `bikes_data_warehouse.fact_journey` AS f
LEFT JOIN `bikes_data_warehouse.dim_locations` AS l
  ON f.end_station_id = l.id
LEFT JOIN `bikes_data_warehouse.dim_timestamp` AS t
  ON f.end_timestamp_id = t.id
WHERE DATE(f.end_timestamp) >= "2021-01-01" AND DATE(f.end_timestamp) <= "2021-12-31" AND t.hour = 3
GROUP BY l.name ORDER BY num_visits DESC
LIMIT 10;

-- Which hour was the most popular for cycling in 2021? 16:00-17:00 is most popular, 4:00-5:00 is least
SELECT t.hour, COUNT(*) AS num_journeys
FROM `bikes_data_warehouse.fact_journey` AS f
LEFT JOIN `bikes_data_warehouse.dim_timestamp` AS t
  ON f.end_timestamp_id = t.id
WHERE DATE(f.end_timestamp) >= "2021-01-01" AND DATE(f.end_timestamp) <= "2021-12-31"
GROUP BY t.hour ORDER BY t.hour;

-- Get a weekly moving average for the weather at a particular location
SELECT w.location_id, w.timestamp,
       AVG(w.tasmax) OVER(ORDER BY UNIX_DATE(DATE(w.timestamp)) RANGE BETWEEN 7 PRECEDING AND CURRENT ROW) AS avg_max_temp,
       AVG(w.tasmin) OVER(ORDER BY UNIX_DATE(DATE(w.timestamp)) RANGE BETWEEN 7 PRECEDING AND CURRENT ROW) AS avg_min_temp,
       AVG(w.rainfall) OVER(ORDER BY UNIX_DATE(DATE(w.timestamp)) RANGE BETWEEN 7 PRECEDING AND CURRENT ROW) AS avg_rainfall,
FROM `bikes_data_warehouse.dim_weather` AS w
WHERE location_id = 1;

-- CTE that creates categorical weather variables
WITH categorical_weather_2021 AS (
  SELECT w.id,
         w.location_id,
         w.timestamp,
         CASE
          WHEN w.rainfall > 1 THEN TRUE
          ELSE FALSE
        END AS is_raining,
        CASE
          WHEN w.tasmax > 30 THEN "very_warm"
          WHEN w.tasmax > 20 THEN "warm"
          ELSE "cold"
        END AS is_warm
  FROM `bikes_data_warehouse.dim_weather` AS w
  WHERE DATE(w.timestamp) >= "2021-01-01" AND DATE(w.timestamp) <= "2021-12-31"
)

-- Are rainy days more likely when it's cold?
SELECT cw.is_warm, cw.is_raining, COUNT(*) AS num_days
FROM categorical_weather_2021 AS cw
WHERE cw.location_id = 1
GROUP BY cw.is_warm, cw.is_raining;

-- How many days were cold or warm for each location?
SELECT cw.location_id, cw.is_warm, COUNT(*) AS num_days
FROM categorical_weather_2021 AS cw
GROUP BY cw.location_id, cw.is_warm ORDER BY cw.location_id;

-- There seem to be a greater number of journeys than expected when it is warm vs. when it is cold
SELECT cw.is_warm, COUNT(*) AS num_journeys
FROM `bikes_data_warehouse.fact_journey` AS f
INNER JOIN categorical_weather_2021 AS cw
  ON f.start_weather_id = cw.id
WHERE DATE(f.start_timestamp) >= "2021-01-01" AND DATE(f.start_timestamp) <= "2021-12-31"
GROUP BY cw.is_warm;
