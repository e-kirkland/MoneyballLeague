# Standard library imports
from ast import literal_eval
import json
import os, sys
from typing_extensions import ParamSpecArgs

# Setting path to sleeper_api_wrapper
module_path = os.path.abspath(os.path.join('../scripts/sleeper_api_wrapper'))
if module_path not in sys.path:
    sys.path.append(module_path)

# Third-party imports
import pandas as pd
import checkpointe as check
import datetime as dt
from sleeper_wrapper import League, Players

# Local imports
from scripts import redis_tools as redis
from scripts import postgres_tools as pgt

def set_league_id(leagueID):
    try:
        return_message = redis.set_league_id(leagueID)
        return return_message
    except Exception as e:
        return e

def get_league_id():
    try:
        leagueID = redis.get_league_id()

        return leagueID

    except Exception as e:
        return e 

def get_players():
    try:
        # Get all player data
        players = Players()
        all_players = players.get_all_players()

        # Convert json to dataframe
        playerdf = pd.DataFrame.from_dict(all_players, orient='index')

        # Format playerdf for postgres upload
        playerdf['player'] = playerdf['full_name']
        keepcols = ['player_id', 'player', 'position', 'team']
        playerdf = playerdf[keepcols]

        # Limiting to only fantasy-relevant players
        offensepos = ['WR','RB','TE','QB','K','DEF']
        playerdf = playerdf[playerdf['position'].isin(offensepos)]

        # Storing placeholders for salary
        playerdf['salary'] = 0

        return playerdf

    except Exception as e:
        return print(e)

def setup_league(leagueID):
    try:
        # Setting league id in cache
        return_message = set_league_id(leagueID)
        print(return_message)

        # Pulling players roster
        playerdf = get_players()

        print(playerdf.head())

        # Get all roster data
        teams = get_teams()

        # Matching players to rosters based on current teams 
        playerdf['roster_id'] = playerdf['player_id'].apply(lambda x: match_player_to_roster(x, teams))

        # Storing player data to postgres
        pgt.df_to_postgres(playerdf, 'postgres', 'playersb', method='replace')

        # Get current transactions
        transactions = get_transactions()

        # Store most recent transaction
        cache_most_recent_transaction(transactions)

        return 'LEAGUE SETUP SUCCESSFULLY'

    except Exception as e:
        return print(e)

def get_transactions():
    try:
        # Get league id
        league_id = get_league_id()
        print(league_id)

        # Instantiate league based on id
        league = League(league_id)

        # Blank transactions value to start
        transactions = []

        # List of possible weeks
        weeklist = range(18, 0, -1)
        print("WEEKLIST: ", weeklist)

        while len(transactions)==0:
            for n in weeklist:
            
                print(f"TRYING TRANSACTIONS FOR WEEK {n}")
                # Try to get transactions for week number
                transactions = league.get_transactions(n)
            # If no transactions found, return none
            break

        return transactions

    except Exception as e:

        return print(e)

def compile_team_data(users, rosters):

    try:

        # # Compile roster data
        rosterdf = pd.DataFrame(rosters)

        print("ROSTER DF LEN: ", len(rosterdf))

        # Narrowing to relevant fields
        keepcols = ['roster_id', 'players', 'owner_id']
        rosterdf = rosterdf[keepcols]

        # Compile user data for each roster
        userdf = pd.DataFrame(users)

        # Narrowing to relevant fields
        keepcols = ['user_id', 'display_name']
        userdf = userdf[keepcols]

        # Compiling both dataframes
        compiled = rosterdf.merge(userdf, "left", left_on='owner_id', right_on='user_id')

        print("COMPILED DF: ", compiled.head())

        keepcols = ['roster_id', 'display_name', 'players']
        compiled = compiled[keepcols]

        # Storing columns for salary_total and players_total
        compiled['salary_total'] = 0
        compiled['players_total'] = 0

        print("FINAL DF: ", compiled.head())

        return compiled

    except Exception as e:

        return e

def get_teams():

    try:

        # Get league id
        league_id = get_league_id()
        print(league_id)

        # Instantiate league based on id
        league = League(league_id)

        # Get data on all league users for team metadata
        users = league.get_users()

        # Get data on all rosters
        rosters = league.get_rosters()

        # Compile team info
        teamsdf = compile_team_data(users, rosters)

        # Post team data to postgres
        pgt.df_to_postgres(teamsdf, 'postgres', 'rosters', method='replace')

        print("ROSTERS POSTED")

        return teamsdf

    except Exception as e:

        return e

def match_player_to_roster(player, rosters):

    match_id = '999'
    for index, row in rosters.iterrows():
        players = row['players']
        if players != None:
            if player in players:
                match_id = row['roster_id']
            else:
                pass
        else:
            pass

    return match_id

def cache_most_recent_transaction(transactions):

    # Get most recent transaction
    transaction = transactions[0]
    transactionid = transaction["transaction_id"]

    print("TRANSACTION: ", transactionid)

    message = redis.set_most_recent_transaction(transactionid)
    print(message)

    return



    