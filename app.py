# Standard library imports
import requests

# Third party imports
from flask import Flask, request, jsonify
from flask_cors import CORS
import checkpointe as check
import redis

# Local imports
from scripts import api_calls as api

# Instantiating app
app = Flask(__name__)
# Enable CORS
cors = CORS(app)

@app.route('/api/test/', methods=['GET'])
def test():
    try:
        return 'SUCCESS', 200
    except Exception as e:
        return f'FAIL {e}'

# Set league ID
@app.route('/api/setupLeague/', methods=['POST'])
def setup_league():

    leagueID = request.args.get('leagueID')

    print(f"SETTING LEAGUE ID AS {leagueID}")
    check.start(summary=True, verbose=True, memory=True)

    try:
        code_json = api.setup_league(leagueID)

        return code_json, 200

    except Exception as e:

        return f"LEAGUE ID SET ERROR: {e} \n Type: {type(e).__name__}", 500

# Get league transactions
@app.route('/api/getTransactions/', methods=['GET'])
def get_transactions():

    try:

        transactions = api.get_transactions()

        return str(transactions), 200

    except Exception as e:

        return f"TRANSACTION RETRIEVAL ERROR {e}", 500


# Get team info
@app.route('/api/getTeams/', methods=['GET'])
def get_teams():

    try:

        teams = api.get_teams()

        return str(teams), 200

    except Exception as e:

        return f"TEAM RETRIEVAL ERROR {e}", 500

if __name__=="__main__":
    app.run(debug=True, host='0.0.0.0', port=3000)