#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri May  5 18:57:28 2023

@author: kareenaparwani
"""

import requests
import pandas as pd
from sklearn.ensemble import RandomForestClassifier

API_KEY = 'beBybSi8daPgsTp5yx5cHtHpYcrjp5Jq'
url = "https://api.polygon.io/v2/aggs/ticker/C:{}{}/range/1/day/2023-01-01/2023-05-01?&apiKey={}"

# define the currency pairs to choose from
currency_pairs = ['NZDCHF', 'GBPUSD', 'USDZAR', 'GBPPLN', 'USDCAD']

# create an empty dataframe to store the historical data
data = pd.DataFrame()

# loop through each currency pair and retrieve the historical data from polygon.io
for pair in currency_pairs:
    response = requests.get(url.format(pair, "", API_KEY))
    json_data = response.json()
    df = pd.DataFrame(json_data['results'])
    df['pair'] = pair
    data = pd.concat([data, df])
    #print(df)

# create a dataframe of features
features = data[['o', 'h', 'l', 'v']]

# create a dataframe of labels
labels = data['pair'].apply(lambda x: currency_pairs.index(x))

# train a random forest classifier
clf = RandomForestClassifier()
clf.fit(features, labels)

# make predictions
predictions = clf.predict(features)

# convert the predicted labels back into currency pairs
predicted_pairs = [currency_pairs[prediction] for prediction in predictions]

# choose the top two predicted currency pairs
top_pairs = sorted(list(set(predicted_pairs)), key=predicted_pairs.count, reverse=True)[:2]

# print the chosen pairs
print("Long on", top_pairs[0])
print("Short on", top_pairs[1])
