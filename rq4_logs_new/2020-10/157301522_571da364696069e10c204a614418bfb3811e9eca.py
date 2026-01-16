import requests
import datetime


def fetch_latest_issues(max_days=7):
    minimum_time = (datetime.date.today() - datetime.timedelta(days=max_days)).strftime(
        "%Y-%m-%d"
    )
    q = 'org:watchdogpolska label:"good first issue" no:assignee is:open updated:>{}'.format(
        minimum_time
    )
    items = requests.get(
        url="https://api.github.com/search/issues",
        params={"q": q, "order_by": "updated_at"},
    ).json()["items"]
    cache = {}
    for item in items:
        if item["repository_url"] not in cache:
            cache[item["repository_url"]] = requests.get(
                url=item["repository_url"]
            ).json()
        item["repository"] = cache[item["repository_url"]]
    return items