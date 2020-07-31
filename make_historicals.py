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
query = {'q': 'algorand',
        'lang': 'en',
         'result_type': 'relevant',
         'count': '100',
        }
currency = 'ALGO'

# iteration index
index = 0

# currency code
currency_code = 'EUR'  # can also use EUR, CAD, etc.

# time interval window
WAIT_WINDOW_IN_WINDOW = 20

DELETE_WINDOW_IN_SECONDS = 86400 # ONE day

def retrieve_and_save(index,df):
    # Search tweets
    dict_ = {'user': [], 'date': [], 'text': []}
    for status in python_tweets.search(**query)['statuses']:
        dict_['user'].append(status['user']['screen_name'])
        dict_['date'].append(status['created_at'])
        dict_['text'].append(status['text'])

    # Make the request
    price = client.rates = client.get_exchange_rates(currency=currency,)

    print ('Current %s price in %s: %s' % (currency, currency_code, price.rates.EUR)) ## Capisci come calcolare percentage

    # Structure data in a pandas DataFrame for easier manipulation
    # df = pd.read_csv("my_result.csv")
    temp = pd.DataFrame(dict_)
    temp.sort_values(by='date', inplace=True, ascending=True)
    temp['index'] = [index for _ in range(0, len(temp))]
    temp['price'] = [price.rates.EUR for _ in range(0, len(temp))]

    df = pd.concat([df, temp])
    df.drop_duplicates(subset=['text','date'], inplace=True) ## delete duplicate

    # write ONLY new ones
    res = df[df['index'] == index]
    res.to_csv('my_result.csv', mode='a', header=False, columns=['index', 'user','date','text', 'price'])

    # drop old tweets - posso anche usare timespan
    Dindex = DELETE_WINDOW_IN_SECONDS / 60*WAIT_WINDOW_IN_WINDOW
    old_index = index - Dindex
    df = df[df['index'] >= old_index ]
    return df
    

if __name__ == "__main__":
    # Search tweets
    dict_ = {'user': [], 'date': [], 'text': []}
    for status in python_tweets.search(**query)['statuses']:
        dict_['user'].append(status['user']['screen_name'])
        dict_['date'].append(status['created_at'])
        dict_['text'].append(status['text'])

    # Make the request
    price = client.rates = client.get_exchange_rates(currency=currency,)

    print ('Current %s price in %s: %s' % (currency, currency_code, price.rates.EUR)) ## Capisci come calcolare percentage

    # Structure data in a pandas DataFrame for easier manipulation
    df = pd.DataFrame(dict_)
    df.sort_values(by='date', inplace=True, ascending=True)
    df['index'] = [index for _ in range(0, len(df))]
    df['price'] = [price.rates.EUR for _ in range(0, len(df))]
    df.to_csv('my_result.csv', columns=['index', 'user','date','text', 'price'])

    while True:
        time.sleep(WAIT_WINDOW_IN_WINDOW * 60)
        index += 1
        df = retrieve_and_save(index, df)
