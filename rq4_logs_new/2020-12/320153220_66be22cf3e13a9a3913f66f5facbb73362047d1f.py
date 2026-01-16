import os 
import re
import requests
import json
import random
from bs4 import BeautifulSoup
from prettytable import PrettyTable

def lambda_handler(event, context):
  p = PrettyTable()
  p.field_names = ["Rank", "Tipper", "Tips", "Margin"]
  p.align['Rank'] = 'r'
  p.align['Tipper'] = 'l'

  REGEX = '{"columns"(.*)'
  MARGIN_REGEX = '(?<=\()[^\)]*'

  # Collect and parse first page
  page = requests.get('https://www.footytips.com.au/compWidget.cfm?c=DPE-2020&gameType=tips&sportId=1&size=300x1800&gameCompId=357818&total=1')
  soup = BeautifulSoup(page.text, 'html.parser')
  script = soup.find("script", attrs={'src': None}) 
  result = re.search(REGEX, script.text)
  data = json.loads(result.group().strip()[:-1])
  for index, record in enumerate(data['records'][0:10]):
    margin = re.search(MARGIN_REGEX, record['total']['info']['label'])
    p.add_row([str(index+1), record['name']['label'], str(record['total']['label']), margin.group()])
  text = ":rugby_football: *DPE Footy Tipping - Top 10 Leaderboard* :rugby_football:\n\n" + f"```{p.get_string()}```"
  blocks = [
        {
        "type": "section",
        "text": {
          "type": "mrkdwn",
          "text": text
        }
      },
      {
        "type": "section",
        "text": {
          "type": "mrkdwn",
          "text": "Don't forget to enter your tips for this week!  :point_right:"
        },
        "accessory": {
          "type": "button",
          "text": {
            "type": "plain_text",
            "text": ":link: Enter Tips",
          },
          "style": "primary",
          "url": "https://www.footytips.com.au/tipping/afl/?ref=comp-header"
        }
      }
  ]
  message = {"text": text, "blocks": blocks}
  test = requests.post(os.environ.get('SLACK_HOOK_URL'), data = json.dumps(message),  headers={'Content-Type': 'application/json'})
  print(test.content)
  print(message)
  return {
    'statusCode': 200,
    'body': json.dumps('Hello from Lambda!')
  }