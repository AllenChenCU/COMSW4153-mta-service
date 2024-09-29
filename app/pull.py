"""Functions to make requests to MTA API and insert into database"""
import requests
import os
from typing import List
from datetime import datetime

import pymysql
import structlog
import pandas as pd

logger = structlog.getLogger(__name__)

EQUIPMENTS_ENDPOINT = "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fnyct_ene_equipments.json"
OUTAGES_ENDPOINT = "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fnyct_ene.json"

INSERT_INTO_EQUIPMENTS_TABLE_QUERY = """
    INSERT INTO equipments (
        timestamp_at_save, 
        station, 
        borough,
        trainno,
        equipmentno, 
        equipmenttype, 
        serving, 
        ADA, 
        isactive,
        nonNYCT,
        shortdescription, 
        linesservedbyelevator,
        elevatorsgtfsstopid, 
        elevatormrn, 
        stationcomplexid, 
        nextadanorth, 
        nextadasouth, 
        redundant,
        busconnections,
        alternativeroute
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    );
"""

ORDERED_EQUIPMENTS_COLS = [
    "timestamp_at_save", "station", "borough", "trainno", "equipmentno",
    "equipmenttype", "serving", "ADA", "isactive", "nonNYCT",
    "shortdescription", "linesservedbyelevator", "elevatorsgtfsstopid", "elevatormrn", "stationcomplexid",
    "nextadanorth", "nextadasouth", "redundant", "busconnections", "alternativeroute"
]

INSERT_INTO_OUTAGES_TABLE_QUERY = """
    INSERT INTO outages (
        timestamp_at_save, 
        station, 
        borough,
        trainno,
        equipment,
        equipmenttype,
        serving,
        ADA,
        outagedate,
        estimatedreturntoservice,
        reason,
        isupcomingoutage,
        ismaintenanceoutage
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    );
"""

ORDERED_OUTAGES_COLS = [
    "timestamp_at_save", "station", "borough", "trainno", "equipment",
    "equipmenttype", "serving", "ADA", "outagedate", "estimatedreturntoservice",
    "reason", "isupcomingoutage", "ismaintenanceoutage"
]


def requests_to_mta_api(api_endpoint: str) -> List[dict]:
    """Make requests to MTA API and returns a list of dict"""
    response = requests.get(api_endpoint)
    return response.json()


def insert_into_table(query, data) -> None:
    """Insert data into the database using the query"""
    try:
        config = {
            "host": os.environ["DBHOST"],
            "user": os.environ["DBUSER"],
            "password": os.environ["DBPASSWORD"],
            "port": int(os.environ["DBPORT"]),
            "db": os.environ["DBNAME"],
        }
        conn = pymysql.connect(**config)
        logger.info("Connected to database.")

        cursor = conn.cursor()
        cursor.executemany(query, data)
        conn.commit()
        logger.info("Inserted data into database.")

    except pymysql.Error as e:
        conn.rollback()
        logger.info(f"An error occurred: {str(e)}")
    finally:
        if conn:
            conn.close()
            logger.info("Database connection is closed.")


def requests_and_saves(api_endpoint, query, col_order) -> str:
    # requests data
    data = requests_to_mta_api(api_endpoint)

    # process data
    current_timestamp = datetime.now()
    formatted_timestamp = current_timestamp.strftime('%Y-%m-%d %H:%M:%S')
    ordered_data = []
    for item in data:
        item['timestamp_at_save'] = formatted_timestamp
        ordered_item = tuple([item[key] for key in col_order])
        ordered_data.append(ordered_item)

    # saves data
    insert_into_table(query, ordered_data)
    return formatted_timestamp


def query_table(query):
    """Run read query"""
    try:
        config = {
            "host": os.environ["DBHOST"],
            "user": os.environ["DBUSER"],
            "password": os.environ["DBPASSWORD"],
            "port": int(os.environ["DBPORT"]),
            "db": os.environ["DBNAME"],
            "cursorclass": pymysql.cursors.DictCursor,
        }
        conn = pymysql.connect(**config)
        logger.info("Connected to database.")

        cursor = conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        results_df = pd.DataFrame(results)

        return results_df

    except pymysql.Error as e:
        conn.rollback()
        logger.info(f"An error occurred: {str(e)}")
    finally:
        if conn:
            conn.close()
            logger.info("Database connection is closed.")


def refresh_data(table):
    """Refresh/repull data if necessary and return the timestamp to query fresh data"""

    # check timestamp
    latest_query = f"""
        SELECT *
        FROM {table}
        ORDER BY timestamp_at_save DESC
        LIMIT 1;
    """
    latest_query_timestamp = query_table(latest_query).loc[0, "timestamp_at_save"]
    current_timestamp = pd.Timestamp.now()
    time_difference = current_timestamp - latest_query_timestamp
    table_refresh_gap = {
        "outages": 1,
        "equipments": 7,
    }
    if time_difference > pd.Timedelta(days=table_refresh_gap[table]):
        if table == "outages":
            timestamp_at_save = requests_and_saves(OUTAGES_ENDPOINT, INSERT_INTO_OUTAGES_TABLE_QUERY, ORDERED_OUTAGES_COLS)
        elif table == "equipments":
            timestamp_at_save = requests_and_saves(EQUIPMENTS_ENDPOINT, INSERT_INTO_EQUIPMENTS_TABLE_QUERY, ORDERED_EQUIPMENTS_COLS)
    else:
        timestamp_at_save = latest_query_timestamp.strftime('%Y-%m-%d %H:%M:%S')
    return timestamp_at_save


if __name__ == "__main__":
    # sanity check
    logger.info("Starting requests and saves for equipments...")
    _ = requests_and_saves(EQUIPMENTS_ENDPOINT, INSERT_INTO_EQUIPMENTS_TABLE_QUERY, ORDERED_EQUIPMENTS_COLS)
    logger.info("Finished.")

    logger.info("Starting requests and saves for outages...")
    _ = requests_and_saves(OUTAGES_ENDPOINT, INSERT_INTO_OUTAGES_TABLE_QUERY, ORDERED_OUTAGES_COLS)
    logger.info("Finished.")
