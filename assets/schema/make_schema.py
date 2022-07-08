import json


def main():

    fact_schema = [
        {"name": "rental_id",          "type": "INTEGER",   "mode": "REQUIRED", "description": "A unique ID for the table."},
        {"name": "start_station_id",   "type": "INTEGER",   "mode": "REQUIRED", "description": "The ID of the station where the journey began."},
        {"name": "end_station_id",     "type": "INTEGER",   "mode": "REQUIRED", "description": "The ID of the station where the journey ended."},
        {"name": "start_weather_id",   "type": "STRING",    "mode": "REQUIRED", "description": "The ID of the weather where/when the journey began."},
        {"name": "end_weather_id",     "type": "STRING",    "mode": "REQUIRED", "description": "The ID of the weather where/when the journey ended."},
        {"name": "start_timestamp_id", "type": "INTEGER",   "mode": "REQUIRED", "description": "The timestamp ID when the journey began."},
        {"name": "end_timestamp_id",   "type": "INTEGER",   "mode": "REQUIRED", "description": "The timestamp ID when the journey ended."},
        {"name": "start_timestamp",    "type": "TIMESTAMP", "mode": "REQUIRED", "description": "The timestamp when the journey began."},
        {"name": "end_timestamp",      "type": "TIMESTAMP", "mode": "REQUIRED", "description": "The timestamp when the journey ended."}
    ]

    json.dump(fact_schema, open("assets/schema/fact_schema.json", "w"))

    weather_schema = [
        {"name": "id",           "type": "STRING",    "mode": "REQUIRED", "description": "A unique ID for the table, based on the location_id and timestamp_id fields."},
        {"name": "location_id",  "type": "INTEGER",   "mode": "REQUIRED", "description": "The ID of the station where the weather observation came from."},
        {"name": "timestamp_id", "type": "INTEGER",   "mode": "REQUIRED", "description": "The timestamp ID for the day of the weather observation."},
        {"name": "timestamp",    "type": "TIMESTAMP", "mode": "REQUIRED", "description": "The time (day) of the weather observation."},
        {"name": "rainfall",     "type": "FLOAT",     "mode": "NULLABLE", "description": "Daily rainfall (mm)."},
        {"name": "tasmin",       "type": "FLOAT",     "mode": "NULLABLE", "description": "Daily minimum temperature (degrees celcius)."},
        {"name": "tasmax",       "type": "FLOAT",     "mode": "NULLABLE", "description": "Daily maximum temperature (degrees celcius)."}
    ]

    json.dump(weather_schema, open("assets/schema/weather_schema.json", "w"))

    rental_schema = [
        {"name": "id",       "type": "INTEGER", "mode": "REQUIRED", "description": "The rental ID."},
        {"name": "bike_id",  "type": "INTEGER", "mode": "REQUIRED", "description": "The bike ID."},
        {"name": "duration", "type": "INTEGER", "mode": "NULLABLE", "description": "The duration the bike was rented for."},
    ]

    json.dump(rental_schema, open("assets/schema/rental_schema.json", "w"))

    # Load raw data in as strings because the formatting is incorrect
    journey_schema = {
        "Rental_Id": "string",
        "Duration": "string",
        "Bike_Id": "string",
        "End_Date": "string",
        "EndStation_Id": "string",
        "EndStation_Name": "string",
        "Start_Date": "string",
        "StartStation_Id": "string",
        "StartStation_Name": "string"
    }

    json.dump(journey_schema, open("assets/schema/journey_schema.json", "w"))


if __name__ == "__main__":
    main()
