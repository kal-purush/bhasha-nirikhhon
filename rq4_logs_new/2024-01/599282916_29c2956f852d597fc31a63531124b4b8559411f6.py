#!/usr/bin/python3
"""
This module contains a function that prints the titles of the
top 10 hottest posts for a specified subreddit
"""

import requests


def recurse(subreddit, hot_list=[], after="", count=0):
    """
    prints the tites of the top 10 hottest posts for a specified subreddit

    subreddit: (string) - the subreddit to be queried
    """
    url = "https://www.reddit.com/r/{}/hot.json".format(subreddit)
    headers = {"User-Agent": "api-practice (by /u/madu-foro)"}
    params = {"after": after, "count": count, "limit": 100}

    reddit_resp = requests.get(url, params=params, headers=headers,
                               allow_redirects=False)

    if reddit_resp.status_code == 404:
        return None

    reddit_response_data = reddit_resp.json().get("data", {})
    after = reddit_response_data.get("after")
    count += reddit_response_data.get("dist")

    children = reddit_response_data.get("children")
    for entry in children:
        hot_list.append(entry.get("data").get("title"))

    if after is not None:
        return recurse(subreddit, hot_list, after, count)
    return hot_list