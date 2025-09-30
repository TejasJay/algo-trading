# -*- coding: utf-8 -*-
"""
Interactive Brokers + lightweight_charts demo (separate queues)
---------------------------------------------------------------
Main fix:
- Use TWO queues: bar_queue and scan_queue, so scan rows never block bar updates.
- historicalData -> bar_queue; scannerData -> scan_queue.
- update_chart drains ONLY bar_queue; display_scan drains ONLY scan_queue.

Also keeps:
- Benign error filtering and 165 "no items retrieved" historical retry.
- Stable overlays: single reusable SMA line (no deletes).
"""

import time
import datetime as dt
import queue
import pandas as pd
from threading import Thread

from lightweight_charts import Chart

# IB API imports
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order
from ibapi.scanner import ScannerSubscription
from ibapi.tag_value import TagValue

# =========================
# CONFIG & GLOBALS
# =========================

# Two dedicated queues to avoid cross-talk
bar_queue = queue.Queue()
scan_queue = queue.Queue()

# Single, reusable indicator line (no deletes)
sma_line = None

INITIAL_SYMBOL = "AAPL"
LIVE_TRADING = False
LIVE_TRADING_PORT = 7496
PAPER_TRADING_PORT = 7497
TRADING_PORT = LIVE_TRADING_PORT if LIVE_TRADING else PAPER_TRADING_PORT

DEFAULT_HOST = "127.0.0.1"
DEFAULT_CLIENT_ID = 7  # avoid 1 if other apps use it

chart = None


# =========================
# UTILS
# =========================

def parse_ib_bar_time(bar_date) -> dt.datetime:
    """Normalize IB bar date (epoch or 'YYYYMMDD  HH:MM:SS') to datetime."""
    try:
        if isinstance(bar_date, (int, float)) or (isinstance(bar_date, str) and bar_date.isdigit()):
            return dt.datetime.fromtimestamp(int(bar_date))
        return dt.datetime.strptime(bar_date, "%Y%m%d  %H:%M:%S")
    except Exception:
        for fmt in ("%Y%m%d %H:%M:%S", "%Y%m%d"):
            try:
                return dt.datetime.strptime(str(bar_date), fmt)
            except Exception:
                pass
        return dt.datetime.now()


# =========================
# IB CLIENT / WRAPPER
# =========================

class PTLClient(EWrapper, EClient):
    """
    - unique request IDs
    - scan workflow flags
    - historical-request registry for 165 retry
    """
    def __init__(self, host, port, client_id):
        EClient.__init__(self, self)

        self._next_req_id = 1000
        self.order_id = None
        self.scan_done = False

        # Track historical requests to enable retries on 165
        # req_id -> dict(symbol, timeframe, useRTH, durationStr, what_to_show)
        self.hist_reqs = {}

        self.connect(host, port, client_id)
        thread = Thread(target=self.run, daemon=True)
        thread.start()

    def next_req_id(self) -> int:
        self._next_req_id += 1
        return self._next_req_id

    # --------- EWrapper overrides

    def error(self, req_id, code, msg):
        """
        Filter benign warnings & retry historical on 165.
        """
        # Suppress expected scanner cancel noise
        if code == 162 and "scanner subscription cancelled" in (msg or "").lower():
            print("(info) Scanner cancelled cleanly.")
            return

        # Suppress benign fractional-share rules warning
        if code == 2176:
            print("(info) Ignoring fractional-share rules warning.")
            return

        # Auto-retry for historical 165: "no items retrieved"
        if code == 165 and "no items retrieved" in (msg or "").lower():
            params = self.hist_reqs.get(req_id)
            if params:
                print(f"(info) Hist {req_id} returned no items; retrying with safer params...")
                symbol = params["symbol"]
                timeframe = params["timeframe"]
                what_to_show = params["what_to_show"]
                duration = params["durationStr"]

                safer_useRTH = False
                if duration.strip().upper() in {"1 D", "2 D"}:
                    end_dt = dt.datetime.utcnow() - dt.timedelta(minutes=10)
                    end_str = end_dt.strftime("%Y%m%d %H:%M:%S")
                    self._request_hist_again(symbol, timeframe, what_to_show,
                                             duration, safer_useRTH, end_str)
                else:
                    shorter = "10 D" if timeframe in ("1 min", "2 mins", "3 mins", "5 mins", "10 mins", "15 mins") else "30 D"
                    self._request_hist_again(symbol, timeframe, what_to_show,
                                             shorter, safer_useRTH, "")
                return

        # Farm info/warnings are ok to just print (incl. 2108 etc.)
        if code in [2104, 2106, 2158, 2108]:
            print(msg)
            return

        print(f"Error {code}: {msg}")

    def _request_hist_again(self, symbol, timeframe, what_to_show, durationStr, useRTH, endDateTime):
        """Issue a replacement historical request with adjusted params."""
        global chart
        if chart:
            chart.spinner(True)
            chart.watermark(symbol)

        c = Contract()
        c.symbol = symbol
        c.secType = "STK"
        c.exchange = "SMART"
        c.currency = "USD"

        new_req_id = self.next_req_id()
        # Remember params under the new req_id
        self.hist_reqs[new_req_id] = {
            "symbol": symbol,
            "timeframe": timeframe,
            "what_to_show": what_to_show,
            "durationStr": durationStr,
            "useRTH": useRTH
        }
        self.reqHistoricalData(
            new_req_id, c,
            endDateTime, durationStr, timeframe, what_to_show,
            useRTH, 2, False, []
        )

    def nextValidId(self, orderId: int):
        super().nextValidId(orderId)
        self.order_id = orderId
        print(f"next valid id is {self.order_id}")

    def historicalData(self, req_id, bar):
        """Enqueue ONLY bars to bar_queue (never mixed with scan rows)."""
        t = parse_ib_bar_time(bar.date)
        data = {
            "time": t,
            "open": bar.open,
            "high": bar.high,
            "low": bar.low,
            "close": bar.close,
            "volume": int(bar.volume)
        }
        bar_queue.put(data)

    def historicalDataEnd(self, reqId, start, end):
        print(f"end of data {start} {end}")
        update_chart()

    def orderStatus(self, order_id, status, filled, remaining, avgFillPrice,
                    permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice):
        print(f"order status {order_id} {status} {filled} {remaining} {avgFillPrice}")

    def scannerData(self, req_id, rank, details, distance, benchmark, projection, legsStr):
        """Enqueue ONLY scan rows to scan_queue (kept separate from bars)."""
        super().scannerData(req_id, rank, details, distance, benchmark, projection, legsStr)
        print("got scanner data")
        print(details.contract)
        row = {
            "secType": details.contract.secType,
            "secId": getattr(details.contract, "secId", ""),
            "exchange": details.contract.primaryExchange,
            "symbol": details.contract.symbol
        }
        print({"type": "scan", **row})
        scan_queue.put(row)

    def scannerDataEnd(self, reqId):
        print("scannerDataEnd")
        self.scan_done = True
        try:
            # Cancelling after end is correct; we suppress 162 in error()
            self.cancelScannerSubscription(reqId)
        except Exception as e:
            print(f"cancelScannerSubscription error: {e}")


# =========================
# IB REQUEST HELPERS
# =========================

def get_bar_data(symbol: str, timeframe: str):
    """
    Request historical data for symbol/timeframe.
    Registers params under req_id so we can retry on 165.
    """
    print(f"getting bar data for {symbol} {timeframe}")

    c = Contract()
    c.symbol = symbol
    c.secType = "STK"
    c.exchange = "SMART"
    c.currency = "USD"
    what_to_show = "TRADES"

    chart.spinner(True)
    chart.watermark(symbol)

    req_id = client.next_req_id()

    # Save initial params for retry logic
    client.hist_reqs[req_id] = {
        "symbol": symbol,
        "timeframe": timeframe,
        "what_to_show": what_to_show,
        "durationStr": "30 D",
        "useRTH": True
    }

    client.reqHistoricalData(
        req_id, c,
        "", "30 D", timeframe, what_to_show,
        True,   # useRTH True initially
        2,      # formatDate
        False,  # keepUpToDate
        []
    )


def do_scan(scan_code: str):
    """Run a market scanner; keep table alive until end."""
    req_id = client.next_req_id()
    client.scan_done = False

    scannerSubscription = ScannerSubscription()
    scannerSubscription.instrument = "STK"
    scannerSubscription.locationCode = "STK.US.MAJOR"
    scannerSubscription.scanCode = scan_code

    tagValues = [
        TagValue("optVolumeAbove", "1000"),
        TagValue("avgVolumeAbove", "10000"),
    ]

    client.reqScannerSubscription(req_id, scannerSubscription, [], tagValues)
    display_scan_until_done()


# =========================
# CHART EVENT HANDLERS
# =========================

def on_search(_chart, searched_string: str):
    get_bar_data(searched_string, chart.topbar["timeframe"].value)
    chart.topbar["symbol"].set(searched_string)


def on_timeframe_selection(_chart):
    print("selected timeframe")
    print(chart.topbar["symbol"].value, chart.topbar["timeframe"].value)
    get_bar_data(chart.topbar["symbol"].value, chart.topbar["timeframe"].value)


def take_screenshot(_key):
    img = chart.screenshot()
    t = time.time()
    path = f"screenshot-{t}.png"
    with open(path, "wb") as f:
        f.write(img)
    print(f"Saved screenshot to {path}")


def place_order(key: str):
    symbol = chart.topbar["symbol"].value.strip().upper()

    # --- Contract (SMART US stock)
    c = Contract()
    c.symbol = symbol
    c.secType = "STK"
    c.currency = "USD"
    c.exchange = "SMART"

    # --- Order (explicitly disable unsupported flags)
    o = Order()
    o.orderType = "MKT"
    o.totalQuantity = 1
    o.action = "BUY" if key == "O" else "SELL" if key == "P" else None
    if o.action is None:
        return

    # Make these explicit to avoid 10268:
    o.eTradeOnly = False
    o.firmQuoteOnly = False

    # Sensible, explicit defaults (not required, but keeps behavior predictable)
    o.tif = "DAY"
    o.outsideRth = False
    o.sweepToFill = False
    o.transmit = True

    # Get a valid order id and place
    client.reqIds(-1)
    time.sleep(0.3)
    if client.order_id is not None:
        print(f"placing {o.action} MKT 1 share of {symbol}")
        client.placeOrder(client.order_id, c, o)
    else:
        print("No valid order_id yet; try again.")



# =========================
# SCAN RENDERING
# =========================

def display_scan_until_done():
    def on_row_click(row):
        chart.topbar["symbol"].set(row["symbol"])
        get_bar_data(row["symbol"], "5 mins")

    table = chart.create_table(
        width=0.4, height=0.5,
        headings=("symbol", "value"),
        widths=(0.7, 0.3),
        alignments=("left", "center"),
        position="left",
        func=on_row_click
    )

    while not client.scan_done:
        try:
            row = scan_queue.get(timeout=0.5)
            sym = row.get("symbol", "")
            if sym:
                table.new_row(sym, "")
        except queue.Empty:
            pass

    print("scanner done")


# =========================
# CHART DATA UPDATE
# =========================

def update_chart():
    """
    Drain ONLY the bar_queue into a DataFrame,
    set it on the chart, and update the single SMA line.
    """
    global sma_line

    bars = []
    while True:
        try:
            item = bar_queue.get_nowait()
            bars.append(item)
        except queue.Empty:
            break

    if not bars:
        print("empty queue (no bars)")
        chart.spinner(False)
        return

    df = pd.DataFrame(bars)

    required = {"time", "open", "high", "low", "close"}
    if not required.issubset(set(df.columns)):
        print(f"DataFrame missing required columns: {required - set(df.columns)}")
        chart.spinner(False)
        return

    print(df.tail(3))
    chart.set(df)

    # ---- Indicators: update or create SMA 50 (no delete)
    try:
        sma = df["close"].rolling(window=50).mean()
        ind_df = pd.DataFrame({"time": df["time"], "SMA 50": sma}).dropna()
        if sma_line is None:
            sma_line = chart.create_line(name="SMA 50")
        sma_line.set(ind_df)
    except Exception as e:
        print(f"SMA line error: {e}")
    finally:
        chart.spinner(False)


# =========================
# MAIN
# =========================

if __name__ == "__main__":
    client = PTLClient(DEFAULT_HOST, TRADING_PORT, DEFAULT_CLIENT_ID)

    chart = Chart(toolbox=True, width=1000, inner_width=0.6, inner_height=1)
    chart.legend(True)

    chart.hotkey("shift", "O", place_order)
    chart.hotkey("shift", "P", place_order)

    chart.topbar.textbox("symbol", INITIAL_SYMBOL)
    chart.topbar.switcher(
        "timeframe",
        ("5 mins", "15 mins", "1 hour"),
        default="5 mins",
        func=on_timeframe_selection
    )

    chart.events.search += on_search

    # Initial bars + one scanner run
    get_bar_data(INITIAL_SYMBOL, "5 mins")
    do_scan("HOT_BY_VOLUME")

    chart.topbar.button("screenshot", "Screenshot", func=take_screenshot)

    chart.show(block=True)
