import oandapyV20
from oandapyV20 import API
from oandapyV20.exceptions import V20Error
from oandapyV20.endpoints.orders import OrderCreate
from oandapyV20.endpoints.orders import OrderCreate
from oandapyV20.endpoints.pricing import PricingInfo
from datetime import datetime, time, timedelta
from time import sleep
import pymongo
import pytz
import pandas as pd
from collections import defaultdict

#%%
# Replace "INSERT_API_KEY_HERE" with your actual API key
api_key = "2e025ebe1e96a9393ff148cf3e9be2f1-890841b6fc0dc889d7ffb41d91166f99"

# Replace "INSERT_ACCOUNT_ID_HERE" with your actual OANDA account ID
account_id = "101-001-25508471-001"

# Initialize the OANDA API client
api = API(access_token=api_key, environment="practice")

#Connect to MongoDB
client = pymongo.MongoClient("mongodb+srv://kareenap:kp3052@cluster0.d3mknmx.mongodb.net/?retryWrites=true&w=majority")

#Initialize MongoDB
db = client.kp3052

# Set the currency pairs and order size
pairs= [
    {"instrument": "USD_CHF", "side": "buy", "units":20000},
    {"instrument": "USD_ZAR", "side": "sell", "units":20000}]

#Variable to store all the transactions for each execution set
final_output={pair["instrument"]: defaultdict(list) for pair in pairs}

#Function to get the current price of the currency pair
def get_curr_price(instrument):
    params = {"instruments": instrument}
    endpoint = PricingInfo(account_id, params=params)
    response = api.request(endpoint)
    price = float(response["prices"][0]["bids"][0]["price"])
    return price

#Function to calculate the average execution price for each execution set
def calculate_avg_price(final_output):
    total_price=0
    if len(final_output)>0:
        for i in range(len(final_output)):
            total_price+=float(final_output[i]["price"])
        avg=total_price/len(final_output)
    else:
        avg=0
    return avg

#Set to Eastern Standard Time
local_tz = pytz.timezone('America/New_York') 
now = datetime.utcnow()
now_local = now.replace(tzinfo=pytz.utc).astimezone(local_tz)

execution_sets = [
    {"start": now_local.replace(hour=15, minute=25, second=0, microsecond=0), "end":  now_local.replace(hour=17, minute=25, second=0, microsecond=0), "total_amount":20000, "hours":2},
    {"start": now_local.replace(hour=19, minute=0, second=0, microsecond=0), "end": now_local.replace(hour=22, minute=0, second=0, microsecond=0), "total_amount":30000, "hours":3},
    {"start": now_local.replace(hour=23, minute=0, second=0, microsecond=0), "end": (now_local + timedelta(days=1)).replace(hour=1, minute=0, second=0, microsecond=0), "total_amount":20000, "hours":2},
    {"start": (now_local + timedelta(days=1)).replace(hour=3, minute=0, second=0, microsecond=0), "end": (now_local + timedelta(days=1)).replace(hour=6, minute=0, second=0, microsecond=0), "total_amount":30000, "hours":3}
]


total_executed_amount={pair["instrument"]: 0 for pair in pairs}
total_amount_6min={pair["instrument"]: 0 for pair in pairs}
executed_amount = {pair["instrument"]: 0 for pair in pairs}
non_executed_amount = {pair["instrument"]: 0 for pair in pairs}
avg_price_list= {pair["instrument"]: {} for pair in pairs} # Contains average execution price for execution set for both the currency pairs
avg_price={pair["instrument"]: 0 for pair in pairs} #Stores the average of the average_price_list
orders_per_hr=10
res={pair["instrument"]: [] for pair in pairs} #Store the final results for both currency pairs

#Store data in the following collections in MongoDB
col1_long=db['Transactions_LONG_2']
col2_long=db['Results_LONG_2']
col1_short=db['Transactions_SHORT_2']
col2_short=db['Results_SHORT_2']


while True:
# Loop through the execution sets and place orders
    for i,execution_set in enumerate(execution_sets):
        # Obtain the start and end times from the execution sets
        start_time = execution_set["start"]
        end_time = execution_set["end"]
        current_time = datetime.now(tz=local_tz)
        if current_time >= start_time and current_time <= end_time:
            for pair in pairs:
                #Second execution set onwards, add the non-executed amount to the total amount and calculate the average price
                if i>=1: 
                    pair["units"]=execution_set['total_amount']+non_executed_amount[pair["instrument"]]
                    non_executed_amount[pair["instrument"]]=0
                    executed_amount[pair["instrument"]]=0
                    #Calculate the average price by averaging the avg_price_list
                    avg_price[pair["instrument"]]=sum(avg_price_list[pair["instrument"]].values())/len(avg_price_list[pair["instrument"]])
                pair["units"]=pair["units"]/execution_set["hours"]
                #Calculate total amount per order for every 6 mins.
                total_amount_6min[pair["instrument"]]=pair["units"]/orders_per_hr 
            while current_time >= start_time and current_time <= end_time:
                for pair in pairs:
                    #Get current price of the currency pair
                    curr_price = get_curr_price(pair["instrument"]) 
                    #Calculate units per order
                    units_per_order=total_amount_6min[pair["instrument"]]/curr_price
                    units_per_order=int(units_per_order)
                    data = { 
                        "order": {
                            "instrument": pair["instrument"],
                            "units": str(units_per_order) if pair["side"] == "buy" else str(-units_per_order),
                            "type": "MARKET",
                            "timeInForce": "FOK",
                            "positionFill": "DEFAULT"
                            }
                        }
                    print("Current price:",curr_price)
                    print("Average price:",avg_price[pair["instrument"]])
        
                    #Second execution set onwards, check if it satisfies the conditions 
                    if i>=1: 
                        if (pair["side"]=="buy" and curr_price >= avg_price[pair["instrument"]]) or (pair["side"]=="sell" and curr_price<= avg_price[pair["instrument"]]):  
                            r = OrderCreate(accountID=account_id, data=data)
                            #Place order if it satisfies the conditions
                            response = api.request(r) 
                            #Add to the executed_amount
                            executed_amount[pair["instrument"]]+=total_amount_6min[pair["instrument"]]
                        else:
                            #Calculate non-executed amount if it does not satisfy the condition
                            non_executed_amount[pair["instrument"]]+=total_amount_6min[pair["instrument"]]
                            continue
                    #First execution set, place all the orders
                    elif i==0: 
                        r = OrderCreate(accountID=account_id, data=data)
                        #Place order if it satisfies the conditions
                        response = api.request(r)
                        #Add to the executed_amount
                        executed_amount[pair["instrument"]]+=total_amount_6min[pair["instrument"]]
                    if "orderFillTransaction" in response:
                        print("Order placed for {} for ${} at {}".format(pair["instrument"],total_amount_6min[pair["instrument"]], datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
                        #Add output data to final_output
                        final_output[pair["instrument"]][i].append({
                            "timestamp": datetime.now(),
                            "order_id": response["orderFillTransaction"]["orderID"],
                            "instrument": pair["instrument"],
                            "side": pair["side"],
                            "amount": units_per_order,
                            "price": response["orderFillTransaction"]["price"]})
                # Wait for 6 minutes before placing the next order
                sleep(360) 
                current_time = datetime.now(tz=local_tz)
            #Calculate executed amount, non-executed amount and average execution price
            for pair in pairs: 
                if executed_amount[pair["instrument"]] or non_executed_amount[pair["instrument"]]:
                    #Calculate total execution amount
                    total_executed_amount[pair["instrument"]]+=executed_amount[pair["instrument"]]
                    if calculate_avg_price(final_output[pair["instrument"]][i]):
                        #Find average execution price of the execution set
                        avg_price_list[pair["instrument"]][i]=calculate_avg_price(final_output[pair["instrument"]][i])
                    #Store executed amount, non-executed amount, average price and total executed amount after each execution set 
                    res[pair["instrument"]].append({"Executed_Amount":executed_amount[pair["instrument"]],"Non-Executed_Amount":non_executed_amount[pair["instrument"]],"Average_price":avg_price[pair["instrument"]],"Total_Executed_Amount":total_executed_amount[pair["instrument"]]})
    #Executes the loop till the current time reaches the end time of the last execution set
    if current_time > execution_sets[-1]["end"]: 
        break

#Insert transactions and results into MongoDB
for pair in pairs:
    if pair["side"]=="buy":
        for i in range(len(final_output[pair["instrument"]])):
            if len(final_output[pair["instrument"]][i])>0:
                col1_long.insert_many(final_output[pair["instrument"]][i])
        col2_long.insert_many(res[pair["instrument"]])
    else:
        for i in range(len(final_output[pair["instrument"]])):
            if len(final_output[pair["instrument"]][i])>0:
                col1_short.insert_many(final_output[pair["instrument"]][i])
        col2_short.insert_many(res[pair["instrument"]])
        
# After the main loop, wait for 30 minutes
sleep(30 * 60)

executed_50_percent = {pair["instrument"]: False for pair in pairs}

col3_long=db['Transactions_LONG_30-60_2']
col3_short=db['Transactions_SHORT_30-60_2']
#Variable to store transactions 
transactions= {pair["instrument"]: [] for pair in pairs}

# After 30 minutes, execute additional trades based on the specified conditions
for pair in pairs:
    curr_price = get_curr_price(pair["instrument"])
    
    # For LONG trades
    if pair["side"] == "buy" and curr_price >= avg_price[pair["instrument"]]:
        #If condition is satisfied buy 50% of non-executed amount
        units_to_buy = int(0.5 * non_executed_amount[pair["instrument"]])
        data = { 
            "order": {
                "instrument": pair["instrument"],
                "units": str(units_to_buy),
                "type": "MARKET",
                "timeInForce": "FOK",
                "positionFill": "DEFAULT"
                }
            }
        r = OrderCreate(accountID=account_id, data=data)
        #Place order
        response = api.request(r)
        #Update non-executed amount
        non_executed_amount[pair["instrument"]] -= units_to_buy
        executed_50_percent[pair["instrument"]]=True
        
        #Store the transaction in a list
        transactions[pair["instrument"]].append({
            "timestamp": datetime.now(),
            "order_id": response["orderFillTransaction"]["orderID"],
            "instrument": pair["instrument"],
            "side": pair["side"],
            "amount": units_to_buy,
            "price": response["orderFillTransaction"]["price"],
            "Non-Executed_Amount":non_executed_amount[pair["instrument"]]})

    # For SHORT trades
    elif pair["side"] == "sell" and curr_price <= avg_price[pair["instrument"]]:
        #If condition is satisfied buy 50% of non-executed amount
        units_to_buy = int(0.5 * non_executed_amount[pair["instrument"]])
        data = { 
            "order": {
                "instrument": pair["instrument"],
                "units": str(-units_to_buy),
                "type": "MARKET",
                "timeInForce": "FOK",
                "positionFill": "DEFAULT"
                }
            }
        r = OrderCreate(accountID=account_id, data=data)
        #Place order
        response = api.request(r)
        #Update non-executed amount
        non_executed_amount[pair["instrument"]] -= units_to_buy
        executed_50_percent[pair["instrument"]]=True
        
        #Store the transaction in a list
        transactions[pair["instrument"]].append({
            "timestamp": datetime.now(),
            "order_id": response["orderFillTransaction"]["orderID"],
            "instrument": pair["instrument"],
            "side": pair["side"],
            "amount": -units_to_buy,
            "price": response["orderFillTransaction"]["price"],
            "Non-Executed_Amount":non_executed_amount[pair["instrument"]]})


# After the first additional trades, wait for 30 more minutes
sleep(30 * 60)

# After 60 minutes, execute additional trades based on the specified conditions
for pair in pairs:
    curr_price = get_curr_price(pair["instrument"])
    non_executed_units = non_executed_amount[pair["instrument"]]

    # For LONG trades
    if pair["side"] == "buy":
        if curr_price >= avg_price[pair["instrument"]] and executed_50_percent[pair["instrument"]]:
            data = { 
                "order": {
                    "instrument": pair["instrument"],
                    "units": str(non_executed_amount[pair["instrument"]]),
                    "type": "MARKET",
                    "timeInForce": "FOK",
                    "positionFill": "DEFAULT"
                    }
                }
            r = OrderCreate(accountID=account_id, data=data)
            response = api.request(r)
            non_executed_amount[pair["instrument"]] = 0
            
            transactions[pair["instrument"]].append({
                "timestamp": datetime.now(),
                "order_id": response["orderFillTransaction"]["orderID"],
                "instrument": pair["instrument"],
                "side": pair["side"],
                "amount": non_executed_amount[pair["instrument"]],
                "price": response["orderFillTransaction"]["price"],
                "Non-Executed_Amount":non_executed_amount[pair["instrument"]]})
            
        elif curr_price < avg_price[pair["instrument"]] and executed_50_percent[pair["instrument"]]:
            data = { 
                "order": {
                    "instrument": pair["instrument"],
                    "units": str(-non_executed_amount[pair["instrument"]]),
                    "type": "MARKET",
                    "timeInForce": "FOK",
                    "positionFill": "DEFAULT"
                    }
                }
            r = OrderCreate(accountID=account_id, data=data)
            response = api.request(r)
            non_executed_amount[pair["instrument"]] = 0
            
            transactions[pair["instrument"]].append({
                "timestamp": datetime.now(),
                "order_id": response["orderFillTransaction"]["orderID"],
                "instrument": pair["instrument"],
                "side": "sell",
                "amount": -non_executed_amount[pair["instrument"]],
                "price": response["orderFillTransaction"]["price"],
                "Non-Executed_Amount":non_executed_amount[pair["instrument"]]})
            
        elif not executed_50_percent[pair["instrument"]] and curr_price < avg_price[pair["instrument"]]:
            total_executed_units = total_executed_amount[pair["instrument"]]
            target_units = int(0.2 * 20000)
            if total_executed_units <= target_units:
                continue    
            units_to_sell = total_executed_units - target_units
            interval_seconds = 60
            units_per_exec = units_to_sell // (interval_seconds // 60)

            for _ in range(interval_seconds // 60):
                data = { 
                    "order": {
                        "instrument": pair["instrument"],
                        "units": str(-units_per_exec),
                        "type": "MARKET",
                        "timeInForce": "FOK",
                        "positionFill": "DEFAULT"
                        }
                    }
                r = OrderCreate(accountID=account_id, data=data)
                response = api.request(r)
                
            transactions[pair["instrument"]].append({
                "timestamp": datetime.now(),
                "order_id": response["orderFillTransaction"]["orderID"],
                "instrument": pair["instrument"],
                "side": "sell",
                "amount": -units_per_exec,
                "price": response["orderFillTransaction"]["price"],
                "Non-Executed_Amount":non_executed_amount[pair["instrument"]]})
                
    # For SHORT trades
    elif pair["side"] == "sell":
        if curr_price <= avg_price[pair["instrument"]] and executed_50_percent[pair["instrument"]]:
            data = { 
                "order": {
                    "instrument": pair["instrument"],
                    "units": str(-non_executed_amount[pair["instrument"]]),
                    "type": "MARKET",
                    "timeInForce": "FOK",
                    "positionFill": "DEFAULT"
                    }
                }
            r = OrderCreate(accountID=account_id, data=data)
            response = api.request(r)
            non_executed_amount[pair["instrument"]] = 0
            
            transactions[pair["instrument"]].append({
                "timestamp": datetime.now(),
                "order_id": response["orderFillTransaction"]["orderID"],
                "instrument": pair["instrument"],
                "side": pair["instrument"],
                "amount": -non_executed_amount[pair["instrument"]],
                "price": response["orderFillTransaction"]["price"],
                "Non-Executed_Amount":non_executed_amount[pair["instrument"]]})
            
        elif curr_price > avg_price[pair["instrument"]] and executed_50_percent[pair["instrument"]]:
            data = { 
                "order": {
                    "instrument": pair["instrument"],
                    "units": str(non_executed_amount[pair["instrument"]]),
                    "type": "MARKET",
                    "timeInForce": "FOK",
                    "positionFill": "DEFAULT"
                    }
                }
            r = OrderCreate(accountID=account_id, data=data)
            response = api.request(r)
            non_executed_amount[pair["instrument"]] = 0
            
            transactions[pair["instrument"]].append({
                "timestamp": datetime.now(),
                "order_id": response["orderFillTransaction"]["orderID"],
                "instrument": pair["instrument"],
                "side": "buy",
                "amount": non_executed_amount[pair["instrument"]],
                "price": response["orderFillTransaction"]["price"],
                "Non-Executed_Amount":non_executed_amount[pair["instrument"]]})
            
        elif not executed_50_percent[pair["instrument"]] and curr_price > avg_price[pair["instrument"]]:
            total_executed_units = total_executed_amount[pair["instrument"]]
            target_units = int(0.2 * 20000)
            if total_executed_units <= target_units:
                continue 
            units_to_buy = total_executed_units - target_units
            interval_seconds = 60
            units_per_exec = units_to_buy // (interval_seconds // 60)
            for _ in range(interval_seconds // 60):
                data = { 
                    "order": {
                        "instrument": pair["instrument"],
                        "units": str(units_per_exec),
                        "type": "MARKET",
                        "timeInForce": "FOK",
                        "positionFill": "DEFAULT"
                        }
                    }
                r = OrderCreate(accountID=account_id, data=data)
                response = api.request(r)
                
            transactions[pair["instrument"]].append({
                "timestamp": datetime.now(),
                "order_id": response["orderFillTransaction"]["orderID"],
                "instrument": pair["instrument"],
                "side": "buy",
                "amount": units_per_exec,
                "price": response["orderFillTransaction"]["price"],
                "Non-Executed_Amount":non_executed_amount[pair["instrument"]]})
                
#Insert transactions into MongoDB 
for pair in pairs:
    if pair["side"]=="buy":
        col3_long.insert_many(transactions[pair["instrument"]])
    else:
        col3_short.insert_many(transactions[pair["instrument"]])

for instrument, data in res.items():
    output_df=pd.DataFrame(data)
    #Save the output DataFrame to a CSV file
    output_df.to_csv(r"/Users/sw08/Desktop/Courses/Data Engineering/Final/"+f"{instrument}_.csv", index=False)
