# coding=utf-8
"""Script for extracting issues and pull request."""

import csv
import requests


# Define parameters for searching specific issues.
issues_url = 'https://api.github.com/search/issues?'
user = 'inasafe'
repo = 'inasafe'
state = 'closed'
date_closed_from = '2016-12-01'
date_closed_until = '2017-02-05'
per_page = 100
queries = (
    'per_page={per_page}&'
    'q=user:{user}+'
    'repo:{repo}+'
    'state:{state}+'
    'closed:{date_closed_from}..{date_closed_until}').\
    format(per_page=per_page,
           user=user,
           repo=repo,
           state=state,
           date_closed_from=date_closed_from,
           date_closed_until=date_closed_until)
request_url = issues_url + queries


def write_issues(result):
    """Write issues from response to csv."""
    if not result.status_code == 200:
        raise Exception(result.status_code)
    for issue in result.json()['items']:
        if not issue['body']:
            body = ''
        else:
            body = issue['body'].encode('utf-8')
        if not issue['title']:
            title = ''
        else:
            title = issue['title'].encode('utf-8')

        csv_out.writerow(
            [issue['closed_at'],
             issue['number'],
             title,
             body])


def get_pages(headers):
    """Get pages from response headers."""
    pages = dict(
        [(rel[6:-1], url[url.index('<') + 1:-1]) for url, rel in
         [link.split(';') for link in
          headers['link'].split(',')]])
    return pages

# First request
response = requests.get(request_url)
pages = get_pages(response.headers)
limit = response.headers['X-RateLimit-Limit']
remaining = response.headers['X-RateLimit-Remaining']

# Progress information
print response.headers
print 'current page: ' + request_url
print 'last_page: ' + pages['last']
print 'request limit: {limit} remaining: {remaining}'.\
    format(limit=limit, remaining=remaining)
print '_____________________________________________________________' \
      '_______________________________________________________________'

# Create csv file
csv_file = '{repo}-issues.csv'.format(repo=repo.replace('/', '-'))
csv_out = csv.writer(open(csv_file, 'wb'))
csv_out.writerow(
    ('Date Closed', 'Ticket Number', 'Title', 'Description'))
write_issues(response)

# If there is another page for the result, go there.
if 'link' in response.headers:
    pages = get_pages(response.headers)
    while 'last' in pages and 'next' in pages:
        response = requests.get(pages['next'])
        write_issues(response)
        limit = response.headers['X-RateLimit-Limit']
        remaining = response.headers['X-RateLimit-Remaining']

        print response.headers
        print 'current page: ' + pages['next']
        print 'last_page: ' + pages['last']
        print 'request limit: {limit} remaining: {remaining}'.\
            format(limit=limit, remaining=remaining)
        print '_____________________________________________________________' \
              '_______________________________________________________________'

        if pages['next'] == pages['last']:
            break

        pages = get_pages(response.headers)