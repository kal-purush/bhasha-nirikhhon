import requests
import json

def getGithubDetails(id):
    url = 'https://api.github.com/users/' + id + '/repos'
    response = requests.get(url)

    reposAndCommits = []

    if (response.status_code != 200):
        print('There is no matching github account with the id ' + id)
        return []
    
    repositories = response.json()

    if (len(repositories) == 0): 
        print('No repositories found for user ' + id)
        return []

    for repository in repositories:
        rName = repository['name']
        numCommits = len(requests.get('https://api.github.com/repos/'+id+'/'+rName+'/commits').json())
        outputString = 'Repo: ' + rName + ' Number of commits: ' + str(numCommits)
        reposAndCommits.append(outputString)
        print(outputString)

    return reposAndCommits

if __name__ == '__main__':
    id = input('Enter Github user ID: ')
    getGithubDetails(id)