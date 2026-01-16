import time
from bs4 import BeautifulSoup, Tag
from playwright.sync_api import Playwright, sync_playwright


def scrape_webpage(url: str, playwright: Playwright, auth_json_path):
    browser = playwright.chromium.launch(headless=False, timeout=60000)
    context = browser.new_context(storage_state=auth_json_path)
    page = context.new_page()
    page.goto(url)
    time.sleep(5) # just to make sure things load
    html = page.content()
    return html


def get_tweets(raw_html: str) -> list[dict]: 
    soup = BeautifulSoup(raw_html, 'html.parser')
    tweets_as_bs4 = soup.find_all("article",{"data-testid": "tweet"}) 

    tweets_as_dict = []
    for i, tweet in enumerate(tweets_as_bs4): 
        # Extract info 
        handle = get_userhandle(tweet)
        content = get_tweet_content(tweet)

        # Create dict with info
        reply = {}
        reply['id'] = i
        reply['handle'] = handle
        reply['reply_content'] = content 
        tweets_as_dict.append(reply)

    return tweets_as_dict

def get_userhandle(tweet: Tag) -> str: 
    username_results = tweet.find_all("div", {"data-testid": "User-Name"})
    results_handle = []
    for username in username_results: 
        handle = username.find("a", {"href": True})["href"] #Finding first result only has been fine. If more than one, they are duplicates.
        handle = handle.replace("/", "")
        results_handle.append(handle)
    if len(results_handle) > 1: 
        raise Exception(f"WARNING: More than one handle found: {results_handle}")
    else: 
        return results_handle[0]

def get_tweet_content(tweet: Tag) -> str:
    content_results = tweet.find_all("div", {"data-testid": "tweetText"})
    results_content = []
    for content in content_results: 
        results_content.append(content.text)
    if len(results_content) > 1: 
        raise Exception(f"WARNING: More than one tweet text found: {results_content}")
    elif len(results_content) == 1: 
        return results_content[0]
    else: 
        return "" #replying with image only leads to blank tweet content


def did_reply(target_user_handle: str, tweets: list[dict]): 
    response_handles = [t["handle"] for t in tweets]
    if target_user_handle in response_handles: 
        return True
    else: 
        return False

def return_users_replies(target_user_handle: str, tweets: list[dict]) -> list[dict]: 
    users_replies = [t for t in tweets if t["handle"] == target_user_handle]
    return users_replies



def did_user_reply(url: str, target_user: str): 
    with sync_playwright() as playwright:
        html = scrape_webpage(url, playwright, 'auth.json')
        all_tweets = get_tweets(html)
        response_bool = did_reply(target_user, all_tweets)
        if response_bool: 
            users_replies = return_users_replies( target_user, all_tweets,)
        else: 
            user_replies = None
        return (response_bool, users_replies)


if __name__ == '__main__':
    url = "https://twitter.com/lina_colucci/status/1679252007490826240"
    target_user = "Dr_ASChaudhari"
    response_bool, users_replies = did_user_reply(url, target_user)
    print(response_bool, users_replies)