import requests
import os
import sys
import subprocess
import time
import signal
from dotenv import load_dotenv
load_dotenv()

args = sys.argv[1:]
if (len(args) == 0):
    print("Error: Python exec was not provided")
    exit(1)
if (len(args) == 1):
    print("Error: No scripts were provided to start")
    exit(1)

python_exec = args[0]
args.pop(0)

currentCommit = ""
if os.path.exists("latestCommit.txt"):
    with open("latestCommit.txt", "r") as file:
        currentCommit = file.read()

running_processes = []
def startProcesses():
    for arg in args:
        #process = subprocess.Popen(['python', arg])
        process = subprocess.Popen([python_exec, arg])
        running_processes.append(process)


def getLatestCommit(username, repo_name):
    url = f'https://api.github.com/repos/{username}/{repo_name}/branches/main'
    headers = {"Authorization": f"Bearer {os.getenv('github_token')}"}

    response = requests.get(url, headers=headers)
    branch_info = response.json()
    last_commit_sha = branch_info['commit']['sha']
    return last_commit_sha

def invokeUpdate():
    for p in running_processes:
        p.terminate()
    subprocess.run("git pull origin main")
    startProcesses()

def handle_interrupt(signum, frame):
    print("Terminating Processes...")
    for p in running_processes:
        p.terminate()
    exit(0)
signal.signal(signal.SIGINT, handle_interrupt)
signal.signal(signal.SIGTERM, handle_interrupt)


startProcesses()
while True:
    print("Running update check...")
    username = 'CCSU-Computer-Science-Club'
    repo_name = 'CS-Club-Discord-Server-Bots'

    latestCommit = getLatestCommit(username, repo_name)

    if currentCommit != latestCommit:
        currentCommit = latestCommit
        invokeUpdate()
        print("Update found...")

    with open("latestCommit.txt", "w") as file:
        file.write(latestCommit)
    time.sleep(10)