#!/usr/bin/env python3
"""Quick raw WebSocket test — prints first 20 messages from Sharekhan."""
import json, sys
from SharekhanApi.sharekhanWebsocket import SharekhanWebSocket  # type: ignore

with open("/home/ubuntu/Stock-Predictor/Tick_Data_Folder/config.json") as f:
    cfg = json.load(f)

token = cfg["access_token"]
sws = SharekhanWebSocket(token)
count = [0]

def on_data(wsapp, message):
    count[0] += 1
    print(f"MSG#{count[0]} type={type(message).__name__} data={repr(message)[:400]}")
    sys.stdout.flush()
    if count[0] >= 10:
        sws.close_connection()

def on_open(wsapp):
    print("OPEN — subscribing to NC7, NC13, NC22 (LTP feed)")
    sys.stdout.flush()
    sws.subscribe({"action": "subscribe", "key": ["feed"], "value": [""]})
    sws.fetchData({"action": "feed", "key": ["ltp"], "value": ["NC7,NC13,NC22"]})

def on_close(wsapp):
    print("CLOSED")

sws.on_open  = on_open
sws.on_data  = on_data
sws.on_close = on_close
sws.connect()
