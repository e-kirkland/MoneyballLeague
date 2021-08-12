# Standard imports
import os

# Third party imports
import redis

# Local imports

# Initialize redis connection - strictRedis ensures strings returned, not bytes
redis_host = os.getenv('REDIS_SERVER')
redis_port = os.getenv('REDIS_PORT')
cache = redis.StrictRedis(
        host=redis_host,
        port=redis_port,
        charset="utf-8",
        decode_responses=True)


def set_league_id(league_id):

    try:
        # Set league id in redis
        cache.set('league_id', league_id)

        return f'League id set: {league_id}'

    except Exception as e:
        return f'League ID failed to set in Redis: {e}'

def get_league_id():

    try:
        # Get league id from cache
        league_id = cache.get('league_id')

        return league_id
    
    except Exception as e:
        return f'League id retrieval error: {e}'

def set_most_recent_transaction(transactionid):

    try:
        # Set transactionid in redis
        cache.set('most_recent_transaction_id', transactionid)

        return f'Most recent transaction id set: {transactionid}'

    except Exception as e:
        return f'Transaciton ID failed to set in Redis: {e}'