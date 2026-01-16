import openai
import os
import pandas as pd

testKey = os.environ.get("API_KEY")
openai.api_key = testKey

# response = openai.ChatCompletion.create(
#   model='gpt-4',
#   messages=[
#     # {"role": "system", "content": "You are an educational mini-game generator."},
#     {"role": "system", "content": prompt},
#     {"role": "user", "content": learningGoals},
#     {"role": "user", "content": subGoals},
#     {"role" : "user", "content" : "Please format the learning goal code, description, game mechanics, game flow, and the sub-goals to be on seperate lines, each minigame should start with MINIGAME"}
#   ]
# )