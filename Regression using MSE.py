#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May  3 13:29:19 2023

@author: kareenaparwani
"""

import requests
import time
import pandas as pd
import numpy as np
from sklearn import svm, model_selection
from sklearn.metrics import mean_squared_error
from sklearn.impute import SimpleImputer

# Define your Polygon.io API key
API_KEY = 'beBybSi8daPgsTp5yx5cHtHpYcrjp5Jq'

# Define the currency pairs you want to trade
currency_pairs = ['NZDCHF', 'CHFEUR', 'EURTRY', 'AUDUSD', 'USDJPY']

# Initialize empty lists to store the historical data for each currency pair
data = []

# Loop over the currency pairs and retrieve their historical price data from Polygon.io
for pair in currency_pairs:
    url = f"https://api.polygon.io/v3/quotes/C:{pair}?apiKey={API_KEY}"
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Error in request for {pair}: {response.status_code} - {response.text}")
        continue
    json_data = response.json().get('results', [])
    #print (json_data)
    if not json_data:
        print(f"No results found for {pair}")
        continue
    df = pd.DataFrame(json_data)
    df['date'] = pd.to_datetime(df['participant_timestamp'], unit='ns')
    df['date'] = df['date'].astype(int) // 10**9  # Convert to Unix timestamp
    df = df[['date', 'bid_price']]
    df.columns = ['date', pair]
    data.append(df)


# Merge the historical data for all currency pairs into a single dataframe
df = data[0]
for i in range(1, len(data)):
    df = pd.merge(df, data[i], on='date', how='left')

#imputer = SimpleImputer(strategy='mean')
#df.iloc[:, 1:] = imputer.fit_transform(df.iloc[:, 1:])
df.drop_duplicates(subset=['date'], inplace=True)
df.dropna(inplace=True)


# Split the data into training and testing sets
train, test = model_selection.train_test_split(df, train_size=0.7, test_size=0.3, random_state=42)
train = train.reset_index(drop=True)
test = test.reset_index(drop=True)

model = svm.SVR(kernel='linear')
model.fit(train.iloc[:, 1:], train.iloc[:, 0])

# Use the model to predict the future price movements of the currency pairs
predictions = model.predict(test.iloc[:, 1:])

# Calculate the mean squared error of the model's predictions
mse = mean_squared_error(test.iloc[:, 0], predictions)
print("Mean Squared Error:", mse)

# Determine the direction of the predicted price movements for each currency pair
predicted_direction = np.where(predictions > test.iloc[:, 0].values.reshape(-1, 1), 1, -1)

# Buy the first currency pair and sell the second pair if the predicted direction is positive
long_position = np.where(predicted_direction > 0, 1, 0)
short_position = np.where(predicted_direction > 0, 0, 1)

# Calculate the returns of the long and short positions for each currency pair
returns = (long_position * (test.iloc[:, 1:] - train.iloc[:, 1:]) / train.iloc[:, 1:]) - (short_position * (test.iloc[:, 1:] - train.iloc[:, 1:]) / train.iloc[:, 1:])
print(returns)

