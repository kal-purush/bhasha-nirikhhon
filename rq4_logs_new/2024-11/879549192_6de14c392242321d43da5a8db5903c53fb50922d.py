import time
import requests
import os

# Replace with your GitHub repository details
OWNER = 'suhas-cog'
REPO = 'python-script'
RUNNER_LABEL = 'pythonscript'
GITHUB_TOKEN = os.getenv('GITHUB')

def get_runner_status():
    url = f'https://api.github.com/repos/{OWNER}/{REPO}/actions/runners'
    headers = {
        'Authorization': f'token {GITHUB_TOKEN}',
        'Accept': 'application/vnd.github.v3+json'
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        runners = response.json().get('runners', [])
        for runner in runners:
            if RUNNER_LABEL in [label['name'] for label in runner['labels']]:
                return runner['status']
    else:
        print(f"Failed to get runners: {response.status_code} - {response.text}")
    return 'offline'

def main():
    while True:
        status = get_runner_status()
        if status == 'idle':
            print("Continue with workflow")
            break
        elif status == 'busy':
            print("Runner is active, sleeping for 20 seconds...")
            time.sleep(20)
        elif status == 'offline':
            print("Runner is offline")
            break
        else:
            print(f"Unknown status: {status}")
            break

if __name__ == "__main__":
    main()