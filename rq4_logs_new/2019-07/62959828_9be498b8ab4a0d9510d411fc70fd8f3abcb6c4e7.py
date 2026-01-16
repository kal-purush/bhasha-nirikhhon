import os

import requests
from bs4 import BeautifulSoup

from ratelimit import limits, sleep_and_retry

# TODO Add multithreading
# could have a thread pool making requests
# would need to ensure total requests by all threads is under 5 a second

def get_submissions(user):
    res = []
    url = "https://codeforces.com/api/user.status"
    response = requests.get(url, params = {'handle': user})
    if response.status_code == 200:
        json = response.json()
        if json['status'] == "OK":
            for submission in json['result']:
                sub = {}
                sub['id'] = submission['id']
                sub['contestId'] = submission['contestId']
                sub['problemName'] = submission['problem']['name']
                sub['problemIndex'] = submission['problem']['index']
                res.append(sub)
    return res

def get_contest_names():
    res = {}
    url = "http://codeforces.com/api/contest.list"
    response = requests.get(url, params = {'gym': 'false'})
    if response.status_code == 200:
        json = response.json()
        if json['status'] == "OK":
            for contest in json['result']:
                res[contest['id']] = contest['name']
    return res

@sleep_and_retry
@limits(calls = 5, period = 1)
def get_submission_code(submission_id, contest_id):
    url = "https://codeforces.com/contest/%s/submission/%s" % (contest_id, submission_id)
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        code_tag = soup.find(id = 'program-source-text')
        code = code_tag.string
        ext = code_tag['class'][1].split('-')[1]
        return code, ext
    else:
        raise Exception("Submission code retrieval for %s -  %s ended in failure (status code = %d" % (submission_id, contest_id, response.status_code))

# if file does not exist, save code with filename as
# [index] - [problem_name].[ext] in folder [root]/Codeforces/[contest_name]/
# else
# traverse submission list in reverse order (from oldest to newest)
# save first submission with git commit of Add submission for [index] - [problem_name]
# save subsequent submissions with git commit of Attempt bugfix
def build_codebook(user, root):
    # create Codeforces folder in pwd
    try:
        os.chdir(root)
        os.mkdir('Codeforces')
        print("Successfully created Codeforces folder...")
    except FileExistsError:
        print("Failed to create Codeforces folder!...")
    submission_list = get_submissions(user)
    if submission_list:
        print("Successfully retrieved submission list...")
    contest_names = get_contest_names()
    if contest_names:
        print("Successfully retrieved contest_names...")
    for submission in submission_list:
        code, ext = get_submission_code(submission['id'], submission['contestId'])

        # Create contest folder
        try:
            os.mkdir(os.path.join('Codeforces', contest_names[submission['contestId']]))
            print("Successfully created folder for contest", contest_names[submission['contestId']])
        except FileExistsError:
            print("Skipped %s since folder already exists..." % contest_names[submission['contestId']])
        
        # generate filename
        filename = "%s - %s.%s" % (submission['problemIndex'], submission['problemName'], ext)
        filepath = os.path.join('Codeforces', contest_names[submission['contestId']], filename)
        if not os.path.isfile(filepath):
            fp = open(filepath, 'w')
            fp.write(code)
            print("Successfully retrieved code for problem %s - %s from contest %s" % (submission['problemIndex'], submission['problemName'], contest_names[submission['contestId']]))
            fp.close()
        else:
            print("Skipped problem %s - %s from contest %s since it already exists" % (submission['problemIndex'], submission['problemName'], contest_names[submission['contestId']]))

root = "/home/akrish13/Documents"
user = "MystikNinja"
build_codebook(user, root)