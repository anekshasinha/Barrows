#!/usr/bin/env python3

import csv
import sqlalchemy

# Connect to the database
engine = sqlalchemy.create_engine("mysql://codetest:swordfish@database/codetest")
connection = engine.connect()

metadata = sqlalchemy.MetaData(engine)

# Define table schema
places = sqlalchemy.Table('places', metadata, autoload=True, autoload_with=engine)
people = sqlalchemy.Table('people', metadata, autoload=True, autoload_with=engine)

# Load data into 'places' table
with open('/data/places.csv') as csv_file:
    reader = csv.reader(csv_file)
    next(reader)  # Skip header
    for row in reader:
        connection.execute(places.insert().values(city=row[0], county=row[1], country=row[2]))

# Load data into 'people' table
with open('/data/people.csv') as csv_file:
    reader = csv.reader(csv_file)
    next(reader)  # Skip header
    for row in reader:
        connection.execute(people.insert().values(
            given_name=row[0],
            family_name=row[1],
            date_of_birth=row[2],
            place_of_birth=row[3]
        ))

print("Data ingestion complete.")

if __name__ == "__main__":
    pass
