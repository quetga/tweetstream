import tweepy, keys
import pandas as pd
from pymongo import MongoClient
from tweetlistener import TweetListener
from geopy import OpenMapQuest
import time
from state_codes import state_codes
import folium

auth = tweepy.OAuthHandler(keys.consumer_key, keys.consumer_secret)
auth.set_access_token(keys.access_token,
keys.access_token_secret)

api = tweepy.API(auth, wait_on_rate_limit=True, 
                 wait_on_rate_limit_notify=True)

senators_df = pd.read_csv('senators.csv')
senators_df['TwitterID'] = senators_df['TwitterID'].astype(str)
pd.options.display.max_columns = 6
senators_df.head()

atlas_client = MongoClient(keys.mongo_connection_string)

db = atlas_client.senators

tweet_limit = 10000
twitter_stream = tweepy.Stream(api.auth,
                               TweetListener(api, db, tweet_limit))

twitter_stream.filter(track=senators_df.TwitterHandle.tolist(),
        follow=senators_df.TwitterID.tolist())

db.tweets.create_index([('$**', 'text')])

tweet_counts = []
for senator in senators_df.TwitterHandle:
    tweet_counts.append(db.tweets.count_documents(
        {"$text": {"$search": senator}}))
    
tweet_counts_df = senators_df.assign(Tweets=tweet_counts)
tweet_counts_df.sort_values(by='Tweets',
                            ascending=False).head(10)

geo = OpenMapQuest(api_key=keys.mapquest_key)

states = tweet_counts_df.State.unique()
states.sort()

locations = []

for state in states:
    processed = False
    delay = .1
    while not processed:
        try:
            locations.append(
                geo.geocode(state_codes[state] + ', USA'))
            print(locations[-1])
            processed = True
        except: # timed out, so wait before trying again
            print('OpenMapQuest service timed out. Waiting.')
            time.sleep(delay)
            delay += .1

tweets_counts_by_state = tweet_counts_df.groupby(
    'State', as_index=False).sum()

tweets_counts_by_state.head()

usmap = folium.Map(location=[39.8283, -98.5795],
                   zoom_start=4, detect_retina=True,
                   tiles='Stamen Toner')

choropleth = folium.Choropleth(geo_data='us-states.json', name='choropleth', data=tweets_counts_by_state, columns=['State', 'Tweets'],key_on='feature.id',fill_color='YlOrRd', fill_opacity=0.7, line_opacity=0.2, 
    legend_name='Tweets by State').add_to(usmap)

layer = folium.LayerControl().add_to(usmap)

sorted_df = tweet_counts_df.sort_values(by='Tweets', ascending=False)

for index, (name, group) in enumerate(sorted_df.groupby('State')):
    strings = [state_codes[name]] # used to assemble popup text

    for s in group.itertuples():
        strings.append(
            f'{s.Name} ({s.Party}); Tweets: {s.Tweets}')

    text = '<br>'.join(strings)
    marker = folium.Marker(
    (locations[index].latitude, locations[index].longitude),
    popup=text)
    marker.add_to(usmap)

usmap.save('SenatorsTweets.html')

