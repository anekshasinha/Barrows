#!/usr/bin/env python3

import json
import sqlalchemy

# Connect to the database
engine = sqlalchemy.create_engine("mysql://codetest:swordfish@database/codetest")
connection = engine.connect()

metadata = sqlalchemy.MetaData(engine)

# Define table schema
places = sqlalchemy.Table('places', metadata, autoload=True, autoload_with=engine)
people = sqlalchemy.Table('people', metadata, autoload=True, autoload_with=engine)

# Query to count people by country
query = sqlalchemy.sql.select([
    places.c.country,
    sqlalchemy.func.count().label('count')
]).select_from(
    people.join(places, people.c.place_of_birth == places.c.city)
).group_by(
    places.c.country
)

result = connection.execute(query).fetchall()

# Write results to a JSON file
summary = [{'country': row[0], 'count': row[1]} for row in result]

with open('/data/summary_output.json', 'w') as json_file:
    json.dump(summary, json_file, indent=4, separators=(',', ':'))

print("Summary generation complete.")

if __name__ == "__main__":
    pass