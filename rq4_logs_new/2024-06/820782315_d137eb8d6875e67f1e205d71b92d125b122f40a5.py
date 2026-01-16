from flask import Flask, jsonify
from flask_cors import CORS
from hltv_async_api import Hltv
import asyncio

app = Flask(__name__)
CORS(app)

hltv = Hltv()

async def fetch_live_matches():
    matches = await hltv.get_matches(future=False, min_rating=0, days=1)
    return matches

async def fetch_match_data():
    matches_info = []
    for match in await hltv.get_matches(future=False, min_rating=0, days=1):
        match_info = await hltv.get_match_info(match["id"], match["team1"], match["team2"], match["event"])
        matches_info.append(match_info)
    return matches_info

async def fetch_matches_len():
    matches_len = len(await hltv.get_matches(future=False, min_rating=0, days=1))
    return matches_len

async def start_background_tasks():
    await asyncio.sleep(0)  # Create a small delay to allow asyncio to initialize
    await fetch_live_matches()
    await fetch_match_data()
    await fetch_matches_len()

@app.route("/api/live_matches")
async def live_matches():
    await start_background_tasks()  # Ensure background tasks are started
    data_live_matches = await fetch_live_matches()
    return jsonify(data_live_matches)

@app.route("/api/matches")
async def match_data():
    await start_background_tasks()  # Ensure background tasks are started
    data_match = await fetch_match_data()
    return jsonify(data_match)

@app.route("/api/matches_len")
async def match_len():
    await start_background_tasks()  # Ensure background tasks are started
    data_len_match = await fetch_matches_len()
    return jsonify(data_len_match)

if __name__ == "__main__":
    asyncio.run(app.run(debug=True))