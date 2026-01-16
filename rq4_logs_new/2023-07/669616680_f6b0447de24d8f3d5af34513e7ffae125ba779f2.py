from playwright.sync_api import Playwright, sync_playwright, expect
import time
import re
import pandas as pd
import os

def scrape_webpage(playwright: Playwright, auth_json_path, url = 'https://twitter.com/mrbeast/status/1682530774103339008?s=46&t=Rlo94dqY20ZzBz4btwDZiQ'):
    
    browser = playwright.chromium.launch(headless=False, timeout=60000)
    context = browser.new_context(storage_state=auth_json_path)
    page = context.new_page()
    page.goto(url)
    time.sleep(5) # just to make sure things load
    html = page.content()
    browser.close()



    # Read dataframe from CSV file if it exists, otherwise create a new one
    if os.path.exists('nametags.csv'):
        df = pd.read_csv('nametags.csv')
    else:
        df = pd.DataFrame(columns=['nametag', 'profile_link', 'verified', 'sent_message'])

    # Find all occurrences of the pattern
    name_tags = re.findall(r'>@([^<]+)<', html)

    for name_tag in name_tags:
        # Check if 'font-face' is in the name tag
        if 'font-face' not in name_tag:
            # Check if nametag already exists in the dataframe
            if name_tag not in df['nametag'].values:
                profile_link = f"https://twitter.com/{name_tag}"
                # Add to dataframe. For the example, let's assume none of the users are verified and no messages have been sent
                df = df.append({'nametag': name_tag, 
                                'profile_link': profile_link, 
                                'verified': False, 
                                'sent_message': False}, ignore_index=True)

    # Save dataframe to CSV
    df.to_csv('nametags.csv', index=False)

with sync_playwright() as playwright:
    scrape_webpage(playwright, auth_json_path='auth.json', url = 'https://twitter.com/mrbeast/status/1682530774103339008?s=46&t=Rlo94dqY20ZzBz4btwDZiQ')    