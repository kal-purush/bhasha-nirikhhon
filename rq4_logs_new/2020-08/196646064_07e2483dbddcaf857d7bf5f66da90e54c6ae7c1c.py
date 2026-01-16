import asyncio
import glob
import json
import os
from pathlib import Path
import time

POLL_DIR = os.path.join(str(Path.home()), ".config/saltbot/polls")
os.makedirs(POLL_DIR, exist_ok=True)


async def monitor_polls(discord_client):
    """ Check poll files for expiry every 5 seconds """
    while True:
        polls = glob.glob(f"{POLL_DIR}/*")
        for poll in polls:
            with open(poll) as stream:
                poll_data = json.loads(stream.read())
            if time.time() > poll_data["expiry"]:
                channel = discord_client.get_channel(poll_data["channel_id"])
                total_votes = 0
                results = {}
                for choice_num in range(len(poll_data["choices"])):
                    total_for_this_choice = len(poll_data["votes"][str(choice_num)])
                    results[choice_num] = total_for_this_choice
                    total_votes += total_for_this_choice
                    
                print(total_votes)
                response = "```Results:\n\n"
                try:
                    for result in results:
                        choice = poll_data["choices"][result]
                        response += f"\t{choice} -> {int(len(poll_data['votes'][str(result)])/total_votes) * 100}%\n"
                except ZeroDivisionError:
                    response = "```No one voted on this poll :("

                await channel.send(f"{response}```")
                os.remove(poll)

        await asyncio.sleep(5)