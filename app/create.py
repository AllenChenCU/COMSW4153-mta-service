"""Create tables in mta_database"""
import os

import pymysql
import structlog


logger = structlog.getLogger(__name__)


CREATE_EQUIPMENTS_TABLE_QUERY = """
    CREATE TABLE IF NOT EXISTS equipments (
        timestamp_at_save DATETIME NOT NULL, 
        station text null, 
        borough text null,
        trainno text null,
        equipmentno text null, 
        equipmenttype text null, 
        serving text null, 
        ADA text null, 
        isactive text null,
        nonNYCT text null,
        shortdescription text null, 
        linesservedbyelevator text null,
        elevatorsgtfsstopid text null, 
        elevatormrn text null, 
        stationcomplexid text null, 
        nextadanorth text null, 
        nextadasouth text null, 
        redundant int null, 
        busconnections text null, 
        alternativeroute text null
    );
"""


CREATE_OUTAGES_TABLE_QUERY = """
    CREATE TABLE IF NOT EXISTS outages (
        timestamp_at_save DATETIME NOT NULL, 
        station text null, 
        borough text null,
        trainno text null,
        equipment text null,
        equipmenttype text null,
        serving text null,
        ADA text null,
        outagedate DATETIME null,
        estimatedreturntoservice DATETIME null,
        reason text null,
        isupcomingoutage text null,
        ismaintenanceoutage text null 
    )
"""


if __name__ == "__main__":
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

        ## --- Equipments table ---
        # delete table
        cursor.execute("""DROP TABLE equipments;""")
        conn.commit()

        # create table
        cursor.execute(CREATE_EQUIPMENTS_TABLE_QUERY)
        conn.commit()
        logger.info("Created equipments table.")
        ## ------------------------

        ## --- Outages table ---
        # delete table
        cursor.execute("""DROP TABLE outages;""")
        conn.commit()

        # create table
        cursor.execute(CREATE_OUTAGES_TABLE_QUERY)
        conn.commit()
        logger.info("Created outages table.")
        ## ------------------------

    except pymysql.Error as e:
        conn.rollback()
        logger.info(f"An error occurred: {str(e)}")
    finally:
        if conn:
            conn.close()
            logger.info("Database connection is closed.")
