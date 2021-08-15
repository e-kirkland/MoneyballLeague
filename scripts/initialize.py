# Standard library imports
import os 

# Third-party imports
from sqlalchemy import create_engine
import pandas as pd
import psycopg2

# Local imports
from scripts import postgres_tools as pgt
from scripts import api_calls as api

# Function to set up database based on league parameters
def setup_database():

    commands = (
    """
    DROP TABLE IF EXISTS settings
    """,
    """
    CREATE TABLE settings (
        league_id INTEGER NOT NULL PRIMARY KEY,
        salary_cap INTEGER NOT NULL,
        roster_min INTEGER NOT NULL,
        roster_max INTEGER NOT NULL,
        transaction_id VARCHAR
    )
    """,
    """
    DROP TABLE IF EXISTS players
    """,
    """
    CREATE TABLE players (
        player_id   VARCHAR NOT NULL PRIMARY KEY,
        player      VARCHAR,
        position    VARCHAR,
        team        VARCHAR,
        salary      INTEGER,
        roster_id   VARCHAR
    )
    """,
    """
    DROP TABLE IF EXISTS rosters
    """,
    """
    CREATE TABLE rosters (
        roster_id       VARCHAR NOT NULL PRIMARY KEY,
        roster          VARCHAR,
        player_ids      VARCHAR,
        salary_total    INTEGER,
        players_total   INTEGER
    )
    """

    )
    try:

        # storing db_path
        db_path = os.getenv('POSTGRES_CONTAINER')
        print("CONNECTING TO POSTGRES AT: ", str(db_path))

        # Accessing table in posgres db
        options = pgt.postgres_connect(db_path, 'postgres')
        cursor = options[1]
        engine = options[0]
        conn = engine.raw_connection()

        # Create tables one by one
        for command in commands:
            cursor.execute(command)

        # Close communication
        cursor.close()

        # Commit changes
        conn.commit()

    except Exception as e:
        print(f"ERROR IN CREATING TABLES: {e}")

    finally:
        if conn is not None:
            conn.close()

def populate_tables(leagueID, salaryCap, rosterMin, rosterMax):

    # Setting league player info from Sleeper
    try:
        transaction_id = api.setup_league(leagueID)

    except Exception as e:

        return f"LEAGUE ID SET ERROR: {e} \n Type: {type(e).__name__}", 500

    try:

        # Initiating settings in table
        col_list = ['league_id', 'salary_cap', 'roster_min', 'roster_max', 'transaction_id']
        data_list = [int(leagueID), int(salaryCap), int(rosterMin), int(rosterMax), str(transaction_id)]

        settingsdf = pd.DataFrame(data=[data_list], columns = col_list)

        print("SETTINGSDF: ", settingsdf.head())

        pgt.df_to_postgres(settingsdf, 'postgres', 'settings', method='replace')

        return "SUCCESS"

    except Exception as e:

        return f"LEAGUE SETTINGS SETUP ERROR {e}"


def setup_league(leagueID, salaryCap, rosterMin, rosterMax):

    # Set up postgres tables
    setup_database()

    # Populate tables with league info
    return_val = populate_tables(leagueID, salaryCap, rosterMin, rosterMax)

    return return_val
