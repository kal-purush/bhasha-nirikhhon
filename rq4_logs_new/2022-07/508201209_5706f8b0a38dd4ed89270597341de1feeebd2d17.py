import scraper
import os
from github import Github

GIT_ACCESS_TOKEN = os.environ["GIT_ACCESS_TOKEN"]
REPO_URL = "https://github.com/jja08111/HansungNotificationServer/"

def createIssue(body: str):
    github = Github(GIT_ACCESS_TOKEN)
    github.get_repo(REPO_URL).create_issue("공지 스크래핑 실패", body=body, labels=["bug"])

def main():
    '''스크래퍼가 작동하지 않는다면 HansungNotificationServer에 이슈를 등록한다.'''
    try:
        result = scraper.scrapeNotices()
        if (result.count < 1):
            raise Exception("스크래핑한 공지가 0개입니다.")
    except BaseException as e:
        createIssue(str(e))

if __name__ == "__main__":
    main()