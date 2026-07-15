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
DAILY_MAX_LOSS_PER_LOT = -1200   # daily loss CEILING per lot ~= 2% of ~Rs.60k/lot capital (Rs.1.2L for 2 lots). Applied x total_lots ONLY (never x max_trades): a ceiling must not scale with trade count.
# --- Fixed-stop winner design (validated 2yr backtest); replaces the old VIX-scaled SL/RR ---
STOP_LOSS_PER_LOT = 750       # flat rupee stop per lot (no VIX scaling)
REWARD_RISK_RATIO = 3.0       # target = REWARD_RISK_RATIO * |stop|, capped at 2x premium (validated)
HALF_BOOK_AT_R = 0.25         # book HALF the position at +0.25R, then move the runner stop to breakeven
if instrument_name == "NIFTY":
    lot_size = 65
    padding = .01
elif instrument_name == "BANKNIFTY":
    lot_size = 30
    padding = .01

total_lots = int(quantity) // lot_size
# Partial booking splits the position into two equal halves, so an EVEN lot count is required.
if int(quantity) % lot_size != 0 or total_lots < 2 or total_lots % 2 != 0:
    print(f"[config] quantity={quantity} resolves to {total_lots} lot(s) of {lot_size}; "
          f"this strategy books half the position and needs an EVEN lot count (>= 2). Exiting.")
    raise SystemExit(1)
half_quantity = (total_lots // 2) * lot_size   # lot-aligned half booked at +0.25R

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
    vix = edge.fetch_ltp(conn, 'NSE', 'India VIX')  # logged for analysis only (no longer drives the SL)

    # Fixed-stop winner design (inlined; no VIX scaling). Both premium-relative guards removed after
    # validation — the premium floor and the 2x-premium target cap each bound 0/352 in the 2yr
    # backtest, and a long option can't lose more than its premium anyway. stop/target are now pure
    # hardcoded functions of the constants:
    stop_loss = -STOP_LOSS_PER_LOT * total_lots
    target = round(abs(stop_loss) * REWARD_RISK_RATIO, 2)
    half_book_trigger = round(HALF_BOOK_AT_R * abs(stop_loss), 2)
    util.notify(
        f"Risk (fixed): SL={round(stop_loss, 2)}, Target={target}, RR={REWARD_RISK_RATIO}, "
        f"HalfBook>= {half_book_trigger} ({HALF_BOOK_AT_R}R)",
        slack_client=slack_client, slack_channel=slack_channel
    )

    strategy = {
    'instrument_name': instrument_name,
    'India Vix': vix,
    'quantity': int(quantity),
    'lot_size': lot_size,
    'long_exit_price': 0,
    'strategy_state': 'active',
    'entry_date': str(datetime.datetime.now().date()),
    'exit_date': '',
    'entry_day_of_week': datetime.datetime.now().strftime('%A'),
    'exit_day_of_week': '',
    'trend' : trend,
    'pcr' : get_pcr(),
    'rsi' : get_rsi(),
    'long_option_symbol' : buy_strike_symbol,
    'long_option_cost' : long_option_cost,
    'stop_loss': stop_loss,
    'target': target,
    'rr': REWARD_RISK_RATIO,
    # --- half-booking tracking (book half at +0.25R, then run the remainder to breakeven) ---
    'half_book_at_r': HALF_BOOK_AT_R,
    'half_book_trigger': half_book_trigger,
    'half_booked': False,
    'half_booked_pnl': 0,
    'half_book_price': 0,
    'half_book_time': '',
    'half_book_date': '',
    'runner_quantity': int(quantity),
    'total_investment': round(long_option_cost * int(quantity), 2),
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


def update_pnl_extremes(strategy, current_pnl):
    current_pnl = round(float(current_pnl), 2)
    prev_max = float(strategy.get('max_pnl_reached', 0) or 0)
    prev_min = float(strategy.get('min_pnl_reached', 0) or 0)
    updates = {'running_pnl': current_pnl}
    if current_pnl > prev_max:
        updates['max_pnl_reached'] = current_pnl
    if current_pnl < prev_min:
        updates['min_pnl_reached'] = current_pnl
    strategies.update_one({'_id': strategy['_id']}, {'$set': updates})


@retry(tries=5, delay=5, backoff=2)
def close_active_positions(reason, ltp=None):
    print(f"Closing active positions {instrument_name}")
    util.notify(f"Closing active positions {instrument_name}",slack_client=slack_client, slack_channel=slack_channel)
    active_strategies = strategies.find({'strategy_state': {'$in': ['active', 'partial']}})
    for strategy in active_strategies:
        runner_quantity = int(strategy.get('runner_quantity', strategy['quantity']))
        sell_order = place_sell_order(strategy['long_option_symbol'], runner_quantity)
        util.notify("Long option leg closed",slack_client=slack_client, slack_channel=slack_channel)
        exit_price = ltp if ltp else sell_order['average_traded_price']
        half_booked_pnl = float(strategy.get('half_booked_pnl', 0) or 0)
        runner_pnl = calculate_pnl(runner_quantity, strategy['long_option_cost'], exit_price)
        gross_pnl = round(half_booked_pnl + runner_pnl, 2)
        net_pnl = round(gross_pnl - (105 * total_lots), 2)  # Deducting brokerage and taxes (flat, conservative)
        prev_max = float(strategy.get('max_pnl_reached', 0) or 0)
        prev_min = float(strategy.get('min_pnl_reached', 0) or 0)
        update_last_exit_time()
        strategies.update_one({'_id': strategy['_id']}, {'$set': {
            'strategy_state': 'closed',
            'exit_date': str(datetime.datetime.now().date()),
            'exit_time': datetime.datetime.now().strftime('%H:%M'),
            'exit_day_of_week': datetime.datetime.now().strftime('%A'),
            'long_exit_price': exit_price,
            'exit_reason': reason,
            'running_pnl': gross_pnl,
            'pnl': gross_pnl,
            'net_pnl': net_pnl,
            'max_pnl_reached': max(prev_max, gross_pnl),
            'min_pnl_reached': min(prev_min, gross_pnl),
        }})
        util.notify(f"Exit [{reason}] half={half_booked_pnl} runner={runner_pnl} gross={gross_pnl} net={net_pnl}",slack_client=slack_client, slack_channel=slack_channel)
        print(f"Exit [{reason}] gross={gross_pnl} net={net_pnl}")
        time.sleep(10)  # Wait for 10 seconds before looking for next entry signal
    return

@retry(tries=5, delay=5, backoff=2)
def get_current_price(strategy, start=None):
    if start is None:
        days_ago = datetime.datetime.now() - timedelta(days=7)
        start = days_ago.replace(hour=9, minute=15, second=0, microsecond=0)
    return edge.get_option_price('NFO', strategy['long_option_symbol'], start, datetime.datetime.today(), 'min')


@retry(tries=5, delay=5, backoff=2)
def book_half(strategy, option_price=None):
    """Book the lot-aligned half of the position at >= +0.25R; the runner then sits at breakeven (cost).
    Records the half-book leg so the DB tracks partial-exit -> full-exit for the trade."""
    full_quantity = int(strategy['quantity'])
    sell_order = place_sell_order(strategy['long_option_symbol'], half_quantity)
    half_book_price = option_price if option_price else sell_order['average_traded_price']
    half_booked_pnl = calculate_pnl(half_quantity, strategy['long_option_cost'], half_book_price)
    runner_quantity = full_quantity - half_quantity
    strategies.update_one({'_id': strategy['_id']}, {'$set': {
        'half_booked': True,
        'strategy_state': 'partial',
        'half_booked_pnl': round(half_booked_pnl, 2),
        'half_book_price': half_book_price,
        'half_book_time': datetime.datetime.now().strftime('%H:%M'),
        'half_book_date': str(datetime.datetime.now().date()),
        'runner_quantity': runner_quantity,
    }})
    util.notify(f"HALF BOOK: sold {half_quantity} @ {half_book_price}, half_booked_pnl={round(half_booked_pnl, 2)}; "
                f"runner {runner_quantity} now at breakeven (cost {strategy['long_option_cost']})",
                slack_client=slack_client, slack_channel=slack_channel)
    print(f"HALF BOOK {half_quantity}@{half_book_price} half_booked_pnl={round(half_booked_pnl, 2)} runner={runner_quantity}")
    return



# @retry(tries=5, delay=5, backoff=2)
def main():
    util.notify(f"{instrument_name} Option Buying bot kicked off",slack_client=slack_client, slack_channel=slack_channel)
    print(f"{instrument_name} Option Buying bot kicked off")
    days_ago = datetime.datetime.now() - timedelta(days=7)
    start = days_ago.replace(hour=9, minute=15, second=0, microsecond=0)
    max_trades = 2  # Winner design uses 2 trades/day (was 3). Revert to 3 to loosen.
    daily_max_loss_limit = DAILY_MAX_LOSS_PER_LOT * total_lots  # Daily loss ceiling (2% of capital). CEILING, not a per-trade allowance -> x total_lots only, NEVER x max_trades. Both the kill switch and the re-entry guard check against this.
    
    # Track the time when the last notification was sent
    last_notification_time = datetime.datetime.now()

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
                if strategies.count_documents({'strategy_state': {'$in': ['active', 'partial']}}) > 0:
                    active_strategies = strategies.find(
                        {'strategy_state': {'$in': ['active', 'partial']}})
                    for strategy in active_strategies:
                        option_price = get_current_price(strategy, start)
                        option_cost = float(strategy['long_option_cost'])
                        full_quantity = int(strategy['quantity'])

                        if not strategy.get('half_booked', False):
                            # ---- Phase 1: full position on a fixed stop; book half at +0.25R ----
                            position_pnl = calculate_pnl(full_quantity, option_cost, option_price)
                            update_pnl_extremes(strategy, position_pnl)

                            if position_pnl <= strategy['stop_loss']:
                                util.notify(f"SL HIT! Current PnL: {position_pnl}",slack_client=slack_client, slack_channel=slack_channel)
                                close_active_positions("SL HIT")
                                break
                            elif position_pnl >= strategy['half_book_trigger']:
                                util.notify(f"Half-book level reached ({strategy.get('half_book_at_r', HALF_BOOK_AT_R)}R). PnL: {position_pnl}",slack_client=slack_client, slack_channel=slack_channel)
                                book_half(strategy)
                            elif current_time >= datetime.time(hour=15, minute=25):
                                util.notify("Time Based SL HIT! Closing positions",slack_client=slack_client, slack_channel=slack_channel)
                                close_active_positions("Time Based SL HIT")
                                util.notify(f"Time based SL Hit for {instrument_name} today, waiting for next trading day",slack_client=slack_client, slack_channel=slack_channel)
                                return
                        else:
                            # ---- Phase 2: runner (half), stop at breakeven, target unchanged ----
                            runner_quantity = int(strategy['runner_quantity'])
                            runner_pnl = calculate_pnl(runner_quantity, option_cost, option_price)
                            half_booked_pnl = float(strategy.get('half_booked_pnl', 0) or 0)
                            position_pnl = round(half_booked_pnl + runner_pnl, 2)
                            update_pnl_extremes(strategy, position_pnl)
                            runner_target_price = option_cost + float(strategy['target']) / full_quantity

                            if option_price <= option_cost:
                                util.notify(f"Runner breakeven stop hit @ {option_price} (cost {option_cost}). Booking remainder.",slack_client=slack_client, slack_channel=slack_channel)
                                close_active_positions("Partial+BE")
                                break
                            elif option_price >= runner_target_price:
                                util.notify(f"Runner TARGET hit @ {option_price} (>= {round(runner_target_price, 2)}).",slack_client=slack_client, slack_channel=slack_channel)
                                close_active_positions("Partial+Target")
                                break
                            elif current_time >= datetime.time(hour=15, minute=25):
                                util.notify("Time exit on runner. Closing remainder.",slack_client=slack_client, slack_channel=slack_channel)
                                close_active_positions("Partial+Time")
                                util.notify(f"Time based exit for {instrument_name} today, waiting for next trading day",slack_client=slack_client, slack_channel=slack_channel)
                                return

                elif strategies.count_documents({'entry_date': str(datetime.datetime.now().date())}) < max_trades:
                    today = str(datetime.datetime.now().date())
                    closed_strategies = strategies.find({'exit_date': today, 'strategy_state': 'closed'})
                    realized_today = 0.0
                    for strategy in closed_strategies:
                        try:
                            realized_today += float(strategy.get('net_pnl', 0) or 0)
                        except (TypeError, ValueError):
                            continue

                    if realized_today <= daily_max_loss_limit:
                        message = f"Daily kill switch active. Realized net PnL today: {realized_today}, limit: {daily_max_loss_limit}. Exiting bot for the day."
                        print(message)
                        util.notify(message, slack_client=slack_client, slack_channel=slack_channel)
                        return

                    # Keep projected-entry guard simple: worst-case = fixed stop from the current SL model.
                    projected_stop_loss = -STOP_LOSS_PER_LOT * total_lots
                    if (realized_today + projected_stop_loss) < daily_max_loss_limit:
                        message = f"No further entries possible today. Realized today: {realized_today}, projected stop: {projected_stop_loss}, limit: {daily_max_loss_limit}. Exiting bot for the day."
                        print(message)
                        util.notify(message, slack_client=slack_client, slack_channel=slack_channel)
                        return

                    if get_color() == 'green' and get_instrument_close() > get_high40() and current_time < datetime.time(hour=15, minute=5) and get_close_time() > datetime.datetime.now().replace(hour=9, minute=20, second=0, microsecond=0) and get_close_time() > get_last_exit_time() and get_pcr() < 0.8:
                        print("Creating Bullish Position")
                        buy_call()
                    elif get_color() == 'red' and get_instrument_close() < get_low40() and current_time < datetime.time(hour=15, minute=5) and get_close_time() > datetime.datetime.now().replace(hour=9, minute=20, second=0, microsecond=0) and get_close_time() > get_last_exit_time() and get_pcr() > 1.2:
                        print("Creating Bearish Position")
                        buy_put()
                    else:
                        print("waiting for entry Signal to create new positions!")
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
