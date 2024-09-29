# COMSW4153-mta-service

Description: This microservice makes requests to the Metropolitan Transportation Authority (MTA) API, and stores requested data into mta_database

Author: Allen Chen (atc2160)

Team: CloudGPT

## API Usage:

### 1. Equipments

Given a station name, this API endpoint returns all equipments (escalators and elevators) associated with the station. 

Link template: 3.84.62.68:5001/equipments/{station}

Example: http://3.84.62.68:5001/equipments/74%20St-Broadway

### 2. Outages

Given a station name, this API endpoint returns all outages (escalators and/or elevators) associated with the station.

Link template: 3.84.62.68:5001/outages/{station}

examples: http://3.84.62.68:5001/outages/74%20St-Broadway

## Data

Requested data is saved into the mta_database database (a RDS hosted on AWS)

- DBHOST: comsw4153-mta-db.cgbo8ymdlz3n.us-east-1.rds.amazonaws.com
- DBNAME: mta_database

Tables:
1. equipments table: all elevators and escalators in all stations (active or not)
2. outages table: equipments outage information
