import os
from tamingnifty import connect_definedge as edge
from tamingnifty import utils as util
from tamingnifty import ta
from datetime import datetime, timedelta
from dateutil import parser
import pandas as pd
pd.set_option('display.max_rows', None)
import time
import os
from retry import retry
from slack_sdk import WebClient
from pymongo import MongoClient
from dotenv import (  # pip install python-dotenv
    find_dotenv,
    load_dotenv,
)

"""
slack_url = os.environ.get('slack_url')
slack_channel = os.environ.get('slack_channel')
CONNECTION_STRING = os.environ.get('CONNECTION_STRING')  #Mongo Connection
trade_end_time = parser.parse(str(os.environ.get('trade_end_time'))).time()
"""
dotenv_file: str = find_dotenv()
load_dotenv(dotenv_file)
slack_channel = "bankniftyoptionbuy"
slack_client = WebClient(token=os.environ.get('slack_token'))
CONNECTION_STRING = os.environ.get('CONNECTION_STRING') #Mongo Connection
trade_end_time = parser.parse("15:28:00").time()
trade_start_time = parser.parse("09:20:01").time()

mongo_client = MongoClient(CONNECTION_STRING)
collection_name = "supertrend"

supertrend_collection = mongo_client['Bots'][collection_name]
instrument_name = "BANKNIFTY"


def get_supertrend_start_date(instrument):
    supertrend = supertrend_collection.find_one({"_id": instrument})
    return supertrend["start_date"]

def get_option_symbol(strike, option_type, dte=5):
    conn = edge.login_to_integrate()
    symbols = list(conn.symbols)
    # Filter for Nifty options: segment is 'NFO', instrument_type is 'OPTIDX', symbol is 'NIFTY'
    nifty_options = [
        s for s in symbols
        if s.get("segment") == "NFO"
        and s.get("instrument_type") == "OPTIDX"
        and s.get("symbol") == instrument_name
    ]
    if not nifty_options:
        print("No Nifty options found.")
        return None, None
    df = pd.DataFrame(nifty_options)
    # Standardize column names for filtering
    df = df.rename(columns={
        'trading_symbol': 'TRADINGSYM',
        'option_type': 'OPTIONTYPE',
        'expiry': 'EXPIRY',
        'strike': 'STRIKE'
    })
    # Convert expiry to datetime
    df['EXPIRY'] = df['EXPIRY'].astype(str).apply(lambda x: x.zfill(8))
    df['EXPIRY'] = pd.to_datetime(df['EXPIRY'], format='%d%m%Y', errors='coerce')
    # Filter by strike, option type, and expiry > dte days from now
    df = df[df['TRADINGSYM'].str.contains(str(strike))]
    df = df[df['OPTIONTYPE'].str.match(option_type)]
    current_date = datetime.now()
    df = df[df['EXPIRY'] > (current_date + timedelta(days=dte))]
    df = df.sort_values(by='EXPIRY', ascending=True)
    if df.empty:
        print("No matching option found after filtering.")
        return None, None
    print("Getting options Symbol...")
    print(f"Symbol: {df['TRADINGSYM'].values[0]} , Expiry: {df['EXPIRY'].values[0]}")
    return df['TRADINGSYM'].values[0], df['EXPIRY'].values[0]


@retry(tries=5, delay=5, backoff=2)
def fetch_oi(conn, trading_symbol: str):
    try:
        quote = edge.fetch_historical_data(conn, 'NFO', trading_symbol, (datetime.now() - timedelta(days=7)), datetime.today(), 'min')
        return quote['oi'].iloc[-1]
    except Exception as e:
        print(f"Exception encountered: {e}. Retrying...")

def pcr(conn,atm=25700,multiple=100):
    atm_strike = atm
    call_oi = 0
    put_oi = 0
    for i in range(5):
        symbol, expiry = get_option_symbol(strike=atm, option_type="CE", dte=5)
        call_oi += fetch_oi(conn, symbol)
        atm += multiple

    for i in range(5):
        symbol, expiry = get_option_symbol(strike=atm_strike, option_type="PE", dte=5)
        put_oi += fetch_oi(conn, symbol)
        atm_strike -= multiple
    pcr_value = put_oi / call_oi if call_oi != 0 else float('inf')
    return round(pcr_value, 2)

def get_high_low(instrument):
    supertrend = supertrend_collection.find_one({"_id": instrument})
    return supertrend["initial_high"], supertrend["initial_low"], supertrend["initial_color"]

#@retry(tries=5, delay=5, backoff=2)
def main():
    print("Supertrend Started")
    util.notify(message="Bank Nifty Supertrend bot has started!", slack_client=slack_client, slack_channel=slack_channel)
    # Track the time when the last notification was sent
    last_notification_time = datetime.now()
    while True:
        current_time = datetime.now().time()
        # Calculate elapsed time since the last notification
        notification_time = datetime.now()

        # Calculate elapsed time since the last notification
        elapsed_time = notification_time - last_notification_time
        print(f"elapsed time: {elapsed_time}")
        if elapsed_time >= timedelta(hours=1):
            util.notify(message=f"{instrument_name} SuperTrend bot for Option Buying is alive!", slack_client=slack_client, slack_channel=slack_channel)
            util.notify(message=f"current time from {instrument_name} OptionBuying: {current_time}", slack_client=slack_client, slack_channel=slack_channel)
            # Update the last notification time
            last_notification_time = notification_time

        if current_time > trade_start_time:
            if instrument_name == "NIFTY":
                trading_symbol = "Nifty 50"
            elif instrument_name == "BANKNIFTY":
                trading_symbol = "Nifty Bank"


            days_ago = get_supertrend_start_date(instrument_name)
            days_ago_datetime = days_ago

            # Add one day
            start = days_ago_datetime + timedelta(days=1)
            start = start.replace(hour=9, minute=15, second=0, microsecond=0)
            end = datetime.today()

            conn = edge.login_to_integrate()
            initial_high, initial_low, initial_color = get_high_low(instrument_name)
            df = ta.renko(conn = conn, exchange = 'NSE', trading_symbol = trading_symbol, start=start, end=datetime.today(), brick_size=.05, last_high=initial_high, last_low=initial_low, initial_color=initial_color, initial_datetime=days_ago)
            print(df.iloc[:20])
            print("\n***** Fetched Renko Data *****\n")
            print(df.iloc[-20:])
            
            # print(heiken3.iloc[-50:])
            high40 = df.iloc[-41:-2]['high'].max()
            low40 = df.iloc[-41:-2]['low'].min()
            df = ta.rsi(df, period=40)
            print(f"40 period High: {high40}, Low: {low40}, RSI: {df.iloc[-1]['rsi']}")

            pcr_value = pcr(conn, atm=util.round_to_nearest(df.iloc[-1]['close'], base=100), multiple=100)
            print(f"PCR Value: {pcr_value}")

            if supertrend_collection.count_documents({"_id": instrument_name}) == 0:
                st = {"_id": instrument_name, "datetime": df.iloc[-1]['datetime'], "color":df.iloc[-1]['color'], "close":df.iloc[-1]['close'], "rsi": df.iloc[-1]['rsi'], "last40_high": high40, "last40_low": low40, "start_date": start, "chart" : "renko", "pcr": pcr_value}
                supertrend_collection.insert_one(st)
            else:
                supertrend_collection.update_one({'_id': instrument_name}, {'$set': {"datetime": df.iloc[-1]['datetime'], "color":df.iloc[-1]['color'], "close":df.iloc[-1]['close'], "rsi": df.iloc[-1]['rsi'], "last40_high": high40, "last40_low": low40, "chart" : "renko", "pcr": pcr_value}})
            
            
            print("repeating loop for Supertrend")
        if current_time > trade_end_time:
            time.sleep(200)
            # Extract the date of the first entry
            df = ta.renko(conn = conn, exchange = 'NSE', trading_symbol = trading_symbol, start=start, end=datetime.today(), brick_size=.05, last_high=initial_high, last_low=initial_low, initial_color=initial_color, initial_datetime=days_ago)
            print("\n***** Fetched Renko Data *****\n")
            print(df.iloc[:20])
            if df['datetime'].iloc[0].date() > days_ago.date():
                first_day = df['datetime'].iloc[0].date()
            else:
                first_day = df['datetime'].iloc[1].date()

            # Filter the DataFrame to include only the entries from the first day
            df_first_day = df[df['datetime'].dt.date == first_day]            
            supertrend_collection.update_one({'_id': instrument_name}, {'$set': {"initial_color": df_first_day.iloc[-1]['color'], "initial_high": df_first_day.iloc[-1]['high'], "initial_low": df_first_day.iloc[-1]['low'], "start_date": df_first_day.iloc[0]['datetime']}})
            return
        
        time.sleep(10)

if __name__ == "__main__":
    main()
