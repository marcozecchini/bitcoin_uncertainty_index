from twython import Twython
import pandas as pd
import json
import time
from coinbase.wallet.client import Client

# coinbase client
client = Client("k8Cxj9k1VtbHuzPP", "Ukqdo6dYisEF1I0DBPRW0jnKtggcmV5m", api_version='YYYY-MM-DD')

# Load credentials from json file
with open("twitter_credentials.json", "r") as file:
    creds = json.load(file)

# Instantiate an object
python_tweets = Twython(creds['CONSUMER_KEY'], creds['CONSUMER_SECRET'])

# Create our query
query = {'q': 'bitcoin',
        'lang': 'en',
         'result_type': 'relevant',
         'count': '100',
        }

# iteration index
index = 0

# currency code
currency_code = 'EUR'  # can also use EUR, CAD, etc.

# time interval window
WAIT_WINDOW = 1


def retrieve_and_save(index):
    # Search tweets
    dict_ = {'user': [], 'date': [], 'text': []}
    for status in python_tweets.search(**query)['statuses']:
        dict_['user'].append(status['user']['screen_name'])
        dict_['date'].append(status['created_at'])
        dict_['text'].append(status['text'])

    # Make the request
    price = client.rates = client.get_exchange_rates(currency='BTC',)

    print ('Current bitcoin price in %s: %s' % (currency_code, price.rates.EUR)) ## Capisci come calcolare percentage

    # Structure data in a pandas DataFrame for easier manipulation
    df = pd.DataFrame(dict_)
    df.sort_values(by='date', inplace=True, ascending=True)
    df['index'] = [index for _ in range(0, len(df))]
    df['price'] = [price.rates.EUR for _ in range(0, len(df))]

    df.to_csv('my_result.csv', mode='a', header=False, columns=['index', 'user','date','text', 'price'])

if __name__ == "__main__":
    # Search tweets
    dict_ = {'user': [], 'date': [], 'text': []}
    for status in python_tweets.search(**query)['statuses']:
        dict_['user'].append(status['user']['screen_name'])
        dict_['date'].append(status['created_at'])
        dict_['text'].append(status['text'])

    # Make the request
    price = client.rates = client.get_exchange_rates(currency='BTC',)

    print ('Current bitcoin price in %s: %s' % (currency_code, price.rates.EUR)) ## Capisci come calcolare percentage

    # Structure data in a pandas DataFrame for easier manipulation
    df = pd.DataFrame(dict_)
    df.sort_values(by='date', inplace=True, ascending=True)
    df['index'] = [index for _ in range(0, len(df))]
    df['price'] = [price.rates.EUR for _ in range(0, len(df))]
    df.to_csv('my_result.csv', columns=['index', 'user','date','text', 'price'])

    while True:
        time.sleep(WAIT_WINDOW * 60)
        index += 1
        retrieve_and_save(index)
