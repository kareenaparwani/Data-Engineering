**Data Engineering Project - Currency Pair Analysis and Automated Trading**

This repository contains the code for a data engineering project developed during my spring semester of my Master of Science studies at the New York University Tandon School of Engineering. The project involves analyzing currency pair data, making predictions, and implementing an automated trading system using Python, polygon.io, and OANDA API.

I have used 3 types of regression analysis to pick the best currency pairs based on historical data obtained from polygon.io -

1. Logistic Regression
Here, I have employed the logistic regression algorithm from scikit-learn to predict the most favorable currency pairs for long and short positions. The code fetches historical data for a set of predefined currency pairs, preprocesses the data, and trains a logistic regression classifier using cross-validation. The trained model is then used to make predictions on the same dataset, and the top two predicted currency pairs are identified for going long and short.

2. Random Forest Algorithm
The code begins by fetching historical price data for a predefined set of currency pairs from Polygon.io. The data is then preprocessed and split into features and labels. The features include the opening price, high price, low price, and volume, while the labels represent the currency pairs themselves.
Next, the code trains a random forest classifier using the features and labels. The random forest algorithm leverages an ensemble of decision trees to make predictions. Once the classifier is trained, it is used to make predictions on the same dataset.
The code converts the predicted labels back into currency pairs and identifies the top two currency pairs with the highest prediction frequency. These pairs are then selected for going long and short positions, respectively.

3. Support Vector Machine
The code fetches historical price data for a predefined set of currency pairs from Polygon.io API. It processes the retrieved data, converting the timestamps to Unix format and organizing it into a single dataframe for further analysis.
Next, the data is split into training and testing sets using the train_test_split function from the model_selection module. The SVR model from the svm module is then trained on the training set, using the currency pair prices as the target variable and the remaining features as input.
After training the SVR model, it is used to predict the future price movements of the currency pairs using the testing set. The code calculates the Mean Squared Error (MSE) to evaluate the performance of the model's predictions.
Based on the predicted price movements, the code determines the direction (positive or negative) of each currency pair. It then makes trading decisions by buying the first currency pair and selling the second pair if the predicted direction is positive. The code calculates the returns of the long and short positions for each currency pair and prints them for further analysis.

**Automated Trading System for OANDA: **

It connects to the OANDA API using the provided API key and accesses the specified OANDA account.
The code fetches the current price of the currency pairs and places orders based on certain conditions defined in the execution sets. It calculates the average execution price for each set and stores it for future use.
The code interacts with a MongoDB database to store transaction data and results. It loops through the execution sets, checks the conditions, places orders, and updates the executed and non-executed amounts.
Transactions are stored in MongoDB collections for both long and short positions, and the results are calculated and stored for each currency pair.
This code provides a framework for automating currency pair trading using the OANDA API and MongoDB. It can be customized and expanded to incorporate additional functionalities or trading algorithms.
