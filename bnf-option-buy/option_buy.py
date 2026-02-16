from pymongo import MongoClient
import os
from tamingnifty import connect_definedge as edge
from tamingnifty import utils as util
import requests
import time
import zipfile
from retry import retry
import io
import datetime 
from datetime import timedelta
from dateutil import parser
import pandas as pd
from slack_sdk import WebClient
pd.set_option('display.max_rows', None)
from dotenv import (  # pip install python-dotenv
    find_dotenv,
    load_dotenv,
)

"""
slack_url = os.environ.get('slack_url')
slack_channel = os.environ.get('slack_channel')
CONNECTION_STRING = os.environ.get('CONNECTION_STRING')  #Mongo Connection
user_name = os.environ.get('user_name')
quantity = os.environ.get('quantity')
trade_start_time = parser.parse("9:29:00").time()
trade_end_time = parser.parse(str(os.environ.get('trade_end_time'))).time()
slack_client = WebClient(token=os.environ.get('slack_client'))
"""
dotenv_file: str = find_dotenv()
load_dotenv(dotenv_file)

slack_channel = "bankniftyoptionbuy"
CONNECTION_STRING = os.environ.get('CONNECTION_STRING')
user_name = os.environ.get('user_name')
trade_start_time = parser.parse("9:20:30").time()
trade_end_time = parser.parse("15:28:00").time()
slack_client = WebClient(token=os.environ.get('slack_token'))
quantity = os.environ.get('quantity')
instrument_name = os.environ.get('instrument_name')
if instrument_name == "NIFTY":
    lot_size = 65
    padding = .01
elif instrument_name == "BANKNIFTY":
    lot_size = 30
    padding = .01

total_lots = int(quantity) / lot_size

mongo_client = MongoClient(CONNECTION_STRING)

strategies_collection_name = instrument_name.lower() + "_" + user_name
orders_collection_name = "orders_" + instrument_name.lower() + "_" + user_name

# trades collection
strategies = mongo_client['Bots'][strategies_collection_name]
orders = mongo_client['Bots'][orders_collection_name]  # orders collection
supertrend_collection = mongo_client['Bots']["supertrend"]

@retry(tries=5, delay=5, backoff=2)
def get_instrument_close():
    #
    supertrend = supertrend_collection.find_one({"_id": instrument_name})
    print(f"{instrument_name} Close: {supertrend['close']}")
    return supertrend['close']

@retry(tries=5, delay=5, backoff=2)
def get_pcr():
    #
    supertrend = supertrend_collection.find_one({"_id": instrument_name})
    print(f"{instrument_name} PCR: {supertrend['pcr']}")
    return supertrend['pcr']

@retry(tries=5, delay=5, backoff=2)
def get_rsi():
    #
    supertrend = supertrend_collection.find_one({"_id": instrument_name})
    print(f"{instrument_name} rsi: {supertrend['rsi']}")
    return supertrend['rsi']

@retry(tries=5, delay=5, backoff=2)
def get_high40():
    #
    supertrend = supertrend_collection.find_one({"_id": instrument_name})
    print(f"{instrument_name} High of last 40 Bricks: {supertrend['last40_high']}")
    return supertrend['last40_high']

def get_low40():
    #
    supertrend = supertrend_collection.find_one({"_id": instrument_name})
    print(f"{instrument_name} Low of last 40 bricks: {supertrend['last40_low']}")
    return supertrend['last40_low']

@retry(tries=5, delay=5, backoff=2)
def get_color():
    #
    supertrend = supertrend_collection.find_one({"_id": instrument_name})
    print(f"{instrument_name} Last Brick Color: {supertrend['color']}")
    return supertrend['color']

@retry(tries=5, delay=5, backoff=2)
def get_close_time():
    #
    supertrend = supertrend_collection.find_one({"_id": instrument_name})
    print(f"{instrument_name} Last Brick Close time: {supertrend['datetime']}")
    return supertrend['datetime']

@retry(tries=5, delay=5, backoff=2)
def get_last_exit_time():
    #
    supertrend = supertrend_collection.find_one({"_id": instrument_name})
    print(f"{instrument_name} Last Brick Close time: {supertrend['lastexittime']}")
    return supertrend['lastexittime']

@retry(tries=5, delay=5, backoff=2)
def update_last_exit_time():
    #
    supertrend_collection.update_one({"_id": instrument_name}, {"$set": {"lastexittime": get_close_time()}})
    return

@retry(tries=5, delay=5, backoff=2)
def place_buy_order(symbol, qty):
    # conn = edge.login_to_integrate(True)
    # io = edge.IntegrateOrders(conn)
    # order = io.place_order(
    #     exchange=conn.EXCHANGE_TYPE_NFO,
    #     order_type=conn.ORDER_TYPE_BUY,
    #     price=0,
    #     price_type=conn.PRICE_TYPE_MARKET,
    #     product_type=conn.PRODUCT_TYPE_NORMAL,
    #     quantity=qty,
    #     tradingsymbol=symbol,
    # )
    # order_id = order['order_id']
    # order = get_order_by_order_id(conn, order_id)
    # print(f"Order Status: {order['order_status']}")
    # if order['order_status'] != "COMPLETE":
    #     time.sleep(2)
    #     order = get_order_by_order_id(conn, order_id)
    #     print(f"Order Status after retry: {order['order_status']}")
    # if order['order_status'] != "COMPLETE":
    #     util.notify(f"Order Message: {order['message']}",slack_client=slack_client, slack_channel=slack_channel)
    #     util.notify(f"Order Failed: {order}",slack_client=slack_client, slack_channel=slack_channel)
    #     orders.insert_one(order)
    #     raise Exception("Error in placing order - " +
    #                 str(order['message']))
    order = {
        "order_id": "25052900010716",
        "last_fill_qty": qty,
        "tradingsymbol": symbol,
        "token": "40470",
        "quantity": qty,
        "price_type": "MARKET",
        "product_type": "NORMAL",
        "order_entry_time": datetime.datetime.now().strftime('%d-%m-%Y %H:%M:%S'),
        "order_status": "COMPLETE",
        "order_type": "BUY",
        "exchange_orderid": "1100000127824637",
        "message": " ",
        "pending_qty": "0",
        "price": "0.00",
        "exchange_time": "29-05-2025 13:08:22",
        "average_traded_price": edge.get_option_price('NFO', symbol, (datetime.datetime.now() - timedelta(days=7)), datetime.datetime.today(), 'min'),
        "exchange": "NFO",
        "filled_qty": qty,
        "disclosed_quantity": "0",
        "validity": "DAY",
        "ordersource": "TRTP"
    }
    print(f"Order placed: {order}")
    util.notify(f"Order placed: {order}",slack_client=slack_client, slack_channel=slack_channel)
    orders.insert_one(order)
    return order


@retry(tries=5, delay=5, backoff=2)
def place_sell_order(symbol, qty):
    # conn = edge.login_to_integrate(True)
    # io = edge.IntegrateOrders(conn)
    # order = io.place_order(
    #     exchange=conn.EXCHANGE_TYPE_NFO,
    #     order_type=conn.ORDER_TYPE_SELL,
    #     price=0,
    #     price_type=conn.PRICE_TYPE_MARKET,
    #     product_type=conn.PRODUCT_TYPE_NORMAL,
    #     quantity=qty,
    #     tradingsymbol=symbol,
    # )
    # order_id = order['order_id']
    # order = get_order_by_order_id(conn, order_id)
    # print(f"Order Status: {order['order_status']}")
    # if order['order_status'] != "COMPLETE":
    #     time.sleep(2)
    #     order = get_order_by_order_id(conn, order_id)
    #     print(f"Order Status after retry: {order['order_status']}")
    # if order['order_status'] != "COMPLETE":
    #     util.notify(f"Order Message: {order['message']}",slack_client=slack_client, slack_channel=slack_channel)
    #     util.notify(f"Order Failed: {order}",slack_client=slack_client, slack_channel=slack_channel)
    #     orders.insert_one(order)
    #     raise Exception("Error in placing order - " +
    #                 str(order['message']))
    order = {
        "order_id": "25052900010716",
        "last_fill_qty": qty,
        "tradingsymbol": symbol,
        "token": "40470",
        "quantity": qty,
        "price_type": "MARKET",
        "product_type": "NORMAL",
        "order_entry_time": datetime.datetime.now().strftime('%d-%m-%Y %H:%M:%S'),
        "order_status": "COMPLETE",
        "order_type": "SELL",
        "exchange_orderid": "1100000127824637",
        "message": " ",
        "pending_qty": "0",
        "price": "0.00",
        "exchange_time": "29-05-2025 13:08:22",
        "average_traded_price": edge.get_option_price('NFO', symbol, (datetime.datetime.now() - timedelta(days=7)), datetime.datetime.today(), 'min'),
        "exchange": "NFO",
        "filled_qty": qty,
        "disclosed_quantity": "0",
        "validity": "DAY",
        "ordersource": "TRTP"
    }
    util.notify(f"Order placed: {order}",slack_client=slack_client, slack_channel=slack_channel)
    orders.insert_one(order)
    return order


@retry(tries=5, delay=5, backoff=2)
def get_order_by_order_id(conn: edge.ConnectToIntegrate, order_id):
    io = edge.IntegrateOrders(conn)
    print(f"Getting order by order ID: {order_id}")
    order = io.order(order_id)
    print(order)
    return order


@retry(tries=5, delay=5, backoff=2)
def get_st_strike():
    if instrument_name == "NIFTY":
        base = 50
    else:
        base = 100
    return util.round_to_nearest(x=get_instrument_close(), base=base)



@retry(tries=5, delay=5, backoff=2)
def load_csv_from_zip(url='https://app.definedgesecurities.com/public/allmaster.zip'):
    column_names = ['SEGMENT', 'TOKEN', 'SYMBOL', 'TRADINGSYM', 'INSTRUMENT TYPE', 'EXPIRY', 'TICKSIZE', 'LOTSIZE', 'OPTIONTYPE', 'STRIKE', 'PRICEPREC', 'MULTIPLIER', 'ISIN', 'PRICEMULT', 'UnKnown']
    # Send a GET request to download the zip file
    response = requests.get(url)
    response.raise_for_status()  # This will raise an exception for HTTP errors
    # Open the zip file from the bytes-like object
    with zipfile.ZipFile(io.BytesIO(response.content)) as thezip:
        # Extract the name of the first CSV file in the zip archive
        csv_name = thezip.namelist()[0]
        # Extract and read the CSV file into a pandas DataFrame
        with thezip.open(csv_name) as csv_file:
            df = pd.read_csv(csv_file, header=None, names=column_names, low_memory=False, on_bad_lines='skip')
    df = df[(df['SEGMENT'] == 'NFO') & (df['INSTRUMENT TYPE'] == 'OPTIDX')]
    df = df[(df['SYMBOL'].str.startswith(instrument_name))]
    df = df[df['SYMBOL'] == instrument_name]
    df['EXPIRY'] = df['EXPIRY'].astype(str).apply(lambda x: x.zfill(8))
    df['EXPIRY'] = pd.to_datetime(df['EXPIRY'], format='%d%m%Y', errors='coerce')
    df = df.sort_values(by='EXPIRY', ascending=True)
    # Return the loaded DataFrame
    return df



@retry(tries=5, delay=5, backoff=2)
def get_option_symbol(strike=19950, option_type = "PE" ):
    df = load_csv_from_zip()
    df = df[df['TRADINGSYM'].str.contains(str(strike))]
    df = df[df['OPTIONTYPE'].str.match(option_type)]
    # Get the current date
    current_date = datetime.datetime.now()
    # Calculate the start and end dates of the current week
    df= df[(df['EXPIRY'] > (current_date + timedelta(days=5)))]
    df = df.head(1)
    print("Getting options Symbol...")
    print(f"Symbol: {df['TRADINGSYM'].values[0]} , Expiry: {df['EXPIRY'].values[0]}")
    return df['TRADINGSYM'].values[0], df['EXPIRY'].values[0]



def buy_put():
    option_type = "PE"
    atm = get_st_strike()
    instrument_close = get_instrument_close()
    strike = atm - 200
    util.notify(f"ATM Strike: {atm}, Buy Strike: {strike}, Instrument Close: {instrument_close}",slack_client=slack_client, slack_channel=slack_channel)
    buy_strike_symbol, expiry = get_option_symbol(strike, option_type)
    print(expiry)
    expiry = str(expiry)
    expiry = parser.parse(expiry).date()
    print(expiry)
    buy_order = place_buy_order(buy_strike_symbol, quantity)
    if buy_order['order_status'] == "COMPLETE":
         util.notify("Buy order placed successfully!",slack_client=slack_client, slack_channel=slack_channel)
    else:
        util.notify(f"Buy order failed: {buy_order['message']}",slack_client=slack_client, slack_channel=slack_channel)
        raise Exception("Error in placing buy order - " + str(buy_order['message']))
    long_option_cost = buy_order['average_traded_price']
    util.notify("BNF PUT Option Long successfull!",slack_client=slack_client, slack_channel=slack_channel)
    record_details_in_mongo(buy_strike_symbol, "Bearish", instrument_close, expiry, long_option_cost)



def record_details_in_mongo(buy_strike_symbol, trend, instrument_close, expiry, long_option_cost):
    conn = edge.login_to_integrate()
    vix = edge.fetch_ltp(conn, 'NSE', 'India VIX')
    strategy = {
    'instrument_name': instrument_name,
    'India Vix': vix,
    'quantity': int(quantity),
    'lot_size': lot_size,
    'long_exit_price': 0,
    'strategy_state': 'active',
    'entry_date': str(datetime.datetime.now().date()),
    'exit_date': '',
    'trend' : trend,
    'pcr' : get_pcr(),
    'long_option_symbol' : buy_strike_symbol,
    'long_option_cost' : long_option_cost,
    'entry_time' : datetime.datetime.now().strftime('%H:%M'),
    'exit_time' : '',
    'instrument_close' : round(instrument_close,2),
    'expiry' : str(expiry),
    'running_pnl' : 0,
    'exit_reason': '',
    'pnl': '',
    'net_pnl': '',
    'max_pnl_reached': 0,
    'min_pnl_reached': 0
    }
    strategies.insert_one(strategy)



def buy_call():
    option_type = "CE"
    atm = get_st_strike()
    instrument_close = get_instrument_close()
    strike = atm + 200
    util.notify(f"ATM Strike: {atm}, Buy Strike: {strike}, Instrument Close: {instrument_close}",slack_client=slack_client, slack_channel=slack_channel)
    buy_strike_symbol, expiry = get_option_symbol(strike, option_type)
    print(expiry)
    expiry = str(expiry)
    expiry = parser.parse(expiry).date()
    print(expiry)
    buy_order = place_buy_order(buy_strike_symbol, quantity)
    if buy_order['order_status'] == "COMPLETE":
         util.notify("Buy order placed successfully!",slack_client=slack_client, slack_channel=slack_channel)
    else:
        util.notify(f"Buy order failed: {buy_order['message']}",slack_client=slack_client, slack_channel=slack_channel)
        raise Exception("Error in placing buy order - " + str(buy_order['message']))
    long_option_cost = buy_order['average_traded_price']
    util.notify("BNF CALL Option Long successfull!",slack_client=slack_client, slack_channel=slack_channel)
    record_details_in_mongo(buy_strike_symbol, "Bullish", instrument_close, expiry, long_option_cost)



def calculate_pnl(quantity, entry, exit):
    pnl = float(quantity) * (float(exit) - float(entry))
    print(f"Realized Gains: {round(pnl, 2)}")
    return round(pnl, 2)


@retry(tries=5, delay=5, backoff=2)
def close_active_positions(reason, ltp=None):
    print(f"Closing active positions {instrument_name}")
    util.notify(f"Closing active positions {instrument_name}",slack_client=slack_client, slack_channel=slack_channel)
    active_strategies = strategies.find({'strategy_state': 'active'})
    for strategy in active_strategies:
        sell_order = place_sell_order(strategy['long_option_symbol'], strategy['quantity'])
        util.notify("Long option leg closed",slack_client=slack_client, slack_channel=slack_channel)
        strategies.update_one({'_id': strategy['_id']}, {'$set': {'strategy_state': 'closed'}})
        strategies.update_one({'_id': strategy['_id']}, {'$set': {'exit_date': str(datetime.datetime.now().date())}})
        strategies.update_one({'_id': strategy['_id']}, {'$set': {'exit_time': datetime.datetime.now().strftime('%H:%M')}})
        #strategies.update_one({'_id': strategy['_id']}, {'$set': {'long_exit_price': sell_order['average_traded_price']}})
        strategies.update_one({'_id': strategy['_id']}, {'$set': {'long_exit_price': ltp if ltp else sell_order['average_traded_price']}})
        strategies.update_one({'_id': strategy['_id']}, {'$set': {'exit_reason': reason}})
        update_last_exit_time()
        pnl = calculate_pnl(strategy['quantity'], strategy['long_option_cost'], ltp if ltp else sell_order['average_traded_price'])
        util.notify(f"Realized Gains: {round(pnl, 2)}",slack_client=slack_client, slack_channel=slack_channel)
        strategies.update_one({'_id': strategy['_id']}, {'$set': {'pnl': pnl}})
        strategies.update_one({'_id': strategy['_id']}, {'$set': {'net_pnl': (pnl-(105*total_lots))}})  # Deducting brokerage and taxes
        util.notify(f"Net PnL after brokerage and taxes: {round((pnl-(105*total_lots)), 2)}",slack_client=slack_client, slack_channel=slack_channel)
        print(f"Realized Gains: {round(pnl, 2)}")
        print(f"Net PnL after brokerage and taxes: {round((pnl-(105*total_lots)), 2)}")
        time.sleep(10)  # Wait for 10 seconds before looking for next entry signal
    return

@retry(tries=5, delay=5, backoff=2)
def get_pnl(strategy, start=None):
    if start is None:
        days_ago = datetime.datetime.now() - timedelta(days=7)
        start = days_ago.replace(hour=9, minute=15, second=0, microsecond=0)
    long_option_cost = edge.get_option_price('NFO', strategy['long_option_symbol'], start, datetime.datetime.today(), 'min')
    current_pnl = calculate_pnl(strategy['quantity'], strategy['long_option_cost'], long_option_cost)
    strategies.update_one({'_id': strategy['_id']}, {'$set': {'running_pnl': current_pnl}})
    return current_pnl



# @retry(tries=5, delay=5, backoff=2)
def main():
    util.notify(f"{instrument_name} Option Buying bot kicked off",slack_client=slack_client, slack_channel=slack_channel)
    print(f"{instrument_name} Option Buying bot kicked off")
    days_ago = datetime.datetime.now() - timedelta(days=7)
    start = days_ago.replace(hour=9, minute=15, second=0, microsecond=0)
    trailing_sl = -750 * total_lots
    
    # Track the time when the last notification was sent
    last_notification_time = datetime.datetime.now()
    max_trades = 3  # Set the maximum number of trades allowed per day
    while True:
        try:
            current_time = datetime.datetime.now().time()
            notification_time = datetime.datetime.now()

            # Calculate elapsed time since the last notification
            elapsed_time = notification_time - last_notification_time
            print(f"elapsed time: {elapsed_time}")
            if elapsed_time >= timedelta(hours=1):
                util.notify(message=f"{instrument_name} Intraday option Buying bot is Alive!", slack_client=slack_client, slack_channel=slack_channel)
                util.notify(message=f"current time from {instrument_name} IntradayOptionBuying: {current_time}", slack_client=slack_client, slack_channel=slack_channel)
                # Update the last notification time
                last_notification_time = notification_time
                
            print(f"current time: {current_time}")
            if current_time > trade_start_time:
                print("Trading Window is active.")
                if strategies.count_documents({'strategy_state': 'active'}) > 0:
                    active_strategies = strategies.find(
                        {'strategy_state': 'active'})
                    for strategy in active_strategies:
                        if strategy['max_pnl_reached'] < get_pnl(strategy, start):
                            max_pnl = get_pnl(strategy, start)                         
                            strategies.update_one({'_id': strategy['_id']}, {'$set': {'max_pnl_reached': max_pnl}})
                            trailing_sl = (-750 * total_lots) + max_pnl
                        
                        if strategy['min_pnl_reached'] > get_pnl(strategy, start):
                            strategies.update_one({'_id': strategy['_id']}, {'$set': {'min_pnl_reached': get_pnl(strategy, start)}})

                        if get_pnl(strategy, start) <= trailing_sl:
                            util.notify(f"SL HIT! Current PnL: {strategy['running_pnl']}",slack_client=slack_client, slack_channel=slack_channel)
                            close_active_positions("SL HIT")
                            trailing_sl = -750 * total_lots
                            break

                        if get_pnl(strategy, start) >= 1500 * total_lots:
                            util.notify(f"Target HIT! Current PnL: {strategy['running_pnl']}",slack_client=slack_client, slack_channel=slack_channel)
                            close_active_positions("Target HIT")
                            trailing_sl = -750 * total_lots
                            break
                        
                        if strategy['trend'] == "Bullish" and get_color() == 'red':
                            util.notify(f"Brick changed to {get_color()}, 1 brick SL hit",slack_client=slack_client, slack_channel=slack_channel)
                            close_active_positions("1 brick SL hit")
                            trailing_sl = -750 * total_lots
                            break

                        if strategy['trend'] == "Bearish" and get_color() == 'green':
                            util.notify(f"Brick changed to {get_color()}, 1 brick SL hit",slack_client=slack_client, slack_channel=slack_channel)
                            close_active_positions("1 brick SL hit")
                            trailing_sl = -750 * total_lots
                            break

                        print(str(datetime.datetime.now().date()))
                        if current_time >= datetime.time(hour=15, minute=25):
                            util.notify("Time Based SL HIT! Closing positions",slack_client=slack_client, slack_channel=slack_channel)
                            close_active_positions("Time Based SL HIT")
                            util.notify(f"Time based SL Hit for {instrument_name} today, waiting for next trading day",slack_client=slack_client, slack_channel=slack_channel)
                            return

                elif strategies.count_documents({'entry_date': str(datetime.datetime.now().date())}) < max_trades:
                    if get_color() == 'green' and get_instrument_close() > get_high40() and current_time < datetime.time(hour=15, minute=5) and get_close_time() > datetime.datetime.now().replace(hour=9, minute=20, second=0, microsecond=0) and get_close_time() > get_last_exit_time() and get_rsi() > 60:
                        print("Creating Bullish Position")
                        buy_call()
                    elif get_color() == 'red' and get_instrument_close() < get_low40() and current_time < datetime.time(hour=15, minute=5) and get_close_time() > datetime.datetime.now().replace(hour=9, minute=20, second=0, microsecond=0) and get_close_time() > get_last_exit_time() and get_rsi() < 40:
                        print("Creating Bearish Position")
                        buy_put()
                    else:
                        print("waiting for entry Signal to create new positions!")
                elif max_trades < 3:
                    print(f"Trade Limit hit for {instrument_name} , waiting limit revival")
                else:
                    util.notify(f" MAX Trade Limit hit for {instrument_name} , exit", slack_client=slack_client, slack_channel=slack_channel)
                    return
        except Exception as e:
            util.notify(f"Exception occurred: {str(e)}", slack_client=slack_client, slack_channel=slack_channel)
        
        if current_time > trade_end_time:
            util.notify("Closing Bell, Bot will exit now",slack_client=slack_client, slack_channel=slack_channel)
            return   
        time.sleep(10)
if __name__ == "__main__":
    main()
