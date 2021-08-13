# Standard library imports
import requests
import re
import sys, os
import json

# Third party imports
from flask import Flask, request, jsonify, Response, make_response
from flask_cors import CORS
import checkpointe as check
import redis
import slack
from slackeventsapi import SlackEventAdapter

# Local imports
from scripts import api_calls as api

# Instantiating app
app = Flask(__name__)
# Enable CORS
cors = CORS(app)

# Instantiating slackbot
slack_event_adapter = SlackEventAdapter("f249d9ef105e738a415ffca11dcff8fc",'/slack/events',app)
client = slack.WebClient(token="xoxb-71093634791-2401906948000-srytssMi3rRaL7OKlFZ1324T")
BOT_ID = client.api_call("auth.test")['user_id']

league_users = {
    "U011KTB2AMQ": 1,
    "U232RJNTH": 2,
    "U9CQGJH0S": 4,
    "UBV333PT8": 6,
    "U236W3RAM": 7,
    "U232X98MS": 8,
    "U9FL1AP2B": 10,
}

# Recognize files created
@slack_event_adapter.on('file_shared')
def file_upload(payload):
    print("FILE FOUND")
    event = payload.get('event', {})
    channel_id = event.get('channel_id')
    user_id = event.get('user_id')
    
    print("EVENT: ", event)

    if event != None and BOT_ID != user_id:

        client.chat_postMessage(channel=channel_id, text='Roster file received. Give me a moment to update the league...')

        file = event['file_id']
        channel = event['channel_id']

        user_client = slack.WebClient(token="xoxp-71093634791-1053929078738-2372330400502-c8d8afaf078ab3086fed818e29bcb71c")
        result = user_client.api_call('files.sharedPublicURL', params={'file':file, 'channel':channel})
        print("RESULT: ", result)

        url = result['file']['url_private_download']
        print("URL: ", url)

        token = 'xoxb-71093634791-2401906948000-srytssMi3rRaL7OKlFZ1324T'
        resp = requests.get(url, headers={'Authorization': 'Bearer %s' % token})

        headers = resp.headers['content-disposition']
        output = 'output/'
        fname = output + re.findall("filename=(.*?);", headers)[0].strip("'").strip('"')

        assert not os.path.exists(fname), print("File already exists. Please remove/rename and re-run")
        out_file = open(fname, mode="wb+")
        print("FNAME: ", fname)
        out_file.write(resp.content)
        out_file.close()

        response = api.reset_salary_data(fname)
        print("RESPONSE: ", response)
    
        client.chat_postMessage(channel=channel_id, text='League updated, you are ready to roll!')

    return Response(), 200

# Message tester
@slack_event_adapter.on('message')
def message(payload):
    event = payload.get('event', {})
    channel_id = event.get('channel')
    user_id = event.get('user')
    text = event.get('text').lower()

    if user_id != None and BOT_ID != user_id:
        if text=='teams':
            client.chat_postMessage(channel=channel_id, text='Retrieving team info, just a moment...')
            api_response = get_teams()
            client.chat_postMessage(channel=channel_id, text=api_response)
        elif 'roster' in text:
            roster_id = league_users[user_id]
            client.chat_postMessage(channel=channel_id, text='Retrieving your roster, just a moment...')
            teamname = api.get_team_name(roster_id)
            client.chat_postMessage(channel=channel_id, text=str(teamname))
            roster = api.get_my_roster(roster_id)
            client.chat_postMessage(channel=channel_id, text=str(roster))
        elif text=='cap':
            roster_id = league_users[user_id]
            client.chat_postMessage(channel=channel_id, text='Calculating your cap space, hang on...') 
            print('USERID: ', user_id)
            cap = api.get_my_cap(roster_id)
            client.chat_postMessage(channel=channel_id, text=str(cap))
        elif text=='users':
            users = event.get('users')
            print("Users: ", users)
        else:
            return 

@app.route('/message-count', methods=['POST'])
def message_count():
    data = request.form
    channel_id = data.get('channel_id')
    user_id = data.get('user_id')
    client.chat_postMessage(channel=channel_id, text='Got your slash command')
    return Response(), 200

@app.route('/roster', methods=['POST'])
def get_roster():
    data = request.form
    print("DATA: ", data)
    channel_id = data.get('channel_id')
    user_id = data.get('user_id')
    text = data.get('text')
    if len(text)!=0:
        roster_id = api.get_roster_id(text)
        client.chat_postMessage(channel=channel_id, text=f'Retrieving the roster, just a moment...')
        teamname = api.get_team_name(roster_id)
        client.chat_postMessage(channel=channel_id, text=str(teamname))
        roster = api.get_my_roster(roster_id)
        client.chat_postMessage(channel=channel_id, 
                                text='',
                                blocks=[
                                    {
                                        "type": "section",
                                        "text": {
                                            "type": "mrkdwn",
                                            "text": roster
                                        }
                                    }
                                ])
        return Response(), 200
    else:
        roster_id = league_users[user_id]
        client.chat_postMessage(channel=channel_id, text='Retrieving your roster, just a moment...')
        teamname = api.get_team_name(roster_id)
        client.chat_postMessage(channel=channel_id, text=str(teamname))
        roster = api.get_my_roster(roster_id)
        client.chat_postMessage(channel=channel_id, 
                                text='',
                                blocks=[
                                    {
                                        "type": "section",
                                        "text": {
                                            "type": "mrkdwn",
                                            "text": roster
                                        }
                                    }
                                ])
        return Response(), 200

@app.route('/cap', methods=['POST'])
def get_cap():
    data = request.form
    print("DATA: ", data)
    channel_id = data.get('channel_id')
    user_id = data.get('user_id')
    text = data.get('text')
    if len(text)!=0:
        roster_id = api.get_roster_id(text)
        client.chat_postMessage(channel=channel_id, text='Calculating the cap space, hang on...') 
        print('USERID: ', user_id)
        teamname = api.get_team_cap(roster_id)
        client.chat_postMessage(channel=channel_id, 
                                text='',
                                blocks=[
                                    {
                                        "type": "section",
                                        "text": {
                                            "type": "mrkdwn",
                                            "text": str(teamname)
                                        }
                                    }
                                ])
        cap = api.get_my_cap(roster_id)
        client.chat_postMessage(channel=channel_id, 
                                text='',
                                blocks=[
                                    {
                                        "type": "section",
                                        "text": {
                                            "type": "mrkdwn",
                                            "text": str(cap)
                                        }
                                    }
                                ])
    else:
        roster_id = league_users[user_id]
        client.chat_postMessage(channel=channel_id, text='Calculating your cap space, hang on...') 
        print('USERID: ', user_id)
        teamname = api.get_team_cap(roster_id)
        client.chat_postMessage(channel=channel_id, 
                                text='',
                                blocks=[
                                    {
                                        "type": "section",
                                        "text": {
                                            "type": "mrkdwn",
                                            "text": teamname
                                        }
                                    }
                                ])
        cap = api.get_my_cap(roster_id)
        client.chat_postMessage(channel=channel_id, 
                                text='',
                                blocks=[
                                    {
                                        "type": "section",
                                        "text": {
                                            "type": "mrkdwn",
                                            "text": str(cap)
                                        }
                                    }
                                ])

@app.route('/salary-reset', methods=['POST'])
def reset_salaries():
    data = request.form
    channel_id = data.get('channel_id')
    user_id = data.get('user_id')
    client.chat_postMessage(channel=channel_id, text='Looks like you want to revise the league salaries. \nRetrieving league data for you to update, just a moment...')
    salarycsvfilename = api.get_salary_csv()

    print("PWD: ", os.getcwd())

    response = client.files_upload(
        channels=channel_id,
        file=salarycsvfilename,
        title="Current League Status"
    )
    print("RESPONSE: ", response)

    client.chat_postMessage(channel=channel_id, text='Here are the current player/roster/salary standings. Edit them and repost here to update the league.')

    return Response(), 200



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