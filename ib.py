# file: ib_test_clean.py
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
import threading, time, datetime, zoneinfo

INFO_CODES = {2104, 2106, 2158}  # farm-OK messages; add more if needed

class App(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)

    # Filter noisy "error" messages that are actually infos
    def error(self, reqId, code, msg, advancedOrderRejectJson=""):
        if code not in INFO_CODES:
            print(f"[IBKR] code={code} reqId={reqId} msg={msg}")

    def currentTime(self, time_: int):
        # Convert epoch -> local time (America/Toronto)
        tz = zoneinfo.ZoneInfo("America/Toronto")
        dt_local = datetime.datetime.fromtimestamp(time_, tz)
        print("IB server time:", dt_local.strftime("%Y-%m-%d %H:%M:%S %Z"))
        self.disconnect()

def run_loop(app): app.run()

if __name__ == "__main__":
    app = App()
    app.connect("127.0.0.1", 7497, clientId=1)  # 7496 for Live; 4002/4001 for Gateway

    thread = threading.Thread(target=run_loop, args=(app,), daemon=True)
    thread.start()

    time.sleep(1)           # give the socket a moment
    app.reqCurrentTime()
    time.sleep(5)           # wait for callback
