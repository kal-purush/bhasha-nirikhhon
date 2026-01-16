from jira import JIRA
import pandas as pd
import requests as rq

TRELLO_KEY = #key
TRELLO_TOKEN = #token
TRELLO_BOARD_ID = #Trello Board Id
TRELLO_API_BASE_URL = 'https://api.trello.com'

jira = JIRA(basic_auth=(
  #email
  , 
  #app password
  ), 
  server=
  #server link i.e. 'https://something.atlassian.net'
  )


class Jira2Trello(object):
    def __init__(self):
        self.lists = {}

    def create_card_in_list(self, ticket_number, ticket_summary, ticket_status, ticket_description):
        if ticket_status not in self.lists:
            self.create_list_on_board(ticket_status)
            self.get_lists_on_board()
        li = self.lists[ticket_status]
        rq.post(f"{TRELLO_API_BASE_URL}/1/cards?idList={li}&key={TRELLO_KEY}&token={TRELLO_TOKEN}",
                json={"name": ticket_number + " " + ticket_summary, "desc": ticket_description})

    def create_list_on_board(self, list_name):
        resp = rq.post(f"{TRELLO_API_BASE_URL}/1/lists?idBoard={TRELLO_BOARD_ID}&key={TRELLO_KEY}&token={TRELLO_TOKEN}",
                       json={"name": list_name})
        print(resp)

    def get_lists_on_board(self):
        lists_resp = rq.get(
            f"{TRELLO_API_BASE_URL}/1/boards/{TRELLO_BOARD_ID}/lists?key={TRELLO_KEY}&token={TRELLO_TOKEN}")
        self.lists = {}
        for li in lists_resp.json():
            self.lists[li['name']] = li['id']

    def archive_all_cards_in_all_lists(self):
        for name, id in self.lists.items():
            rq.post(f"{TRELLO_API_BASE_URL}/1/lists/{id}/archiveAllCards?key={TRELLO_KEY}&token={TRELLO_TOKEN}")

    def export_jira_issues(self):
        issues = jira.search_issues(
            'project = FOOD AND (fixVersion in unreleasedVersions() OR updated > -1w) '
            + 'ORDER BY updated ASC, priority DESC',
            maxResults=False,
            fields='key,status,summary,description')
        return pd.DataFrame(self.jira_issue_to_dict(issue) for issue in issues)

    def jira_issue_to_dict(self, issue):
        return {'Issue key': issue.key, 'Summary': issue.fields.summary, 'Status': issue.fields.status.name,
                'Description': issue.fields.description}

    def import_data(self):
        print("Exporting jira issues from JIRA")
        df = self.export_jira_issues()
        print("Exported issues")
        self.get_lists_on_board()
        print("Archiving all cards on Trello board")
        self.archive_all_cards_in_all_lists()
        print("Creating cards on Trello board")
        for row in df.iterrows():
            self.create_card_in_list(row[1].get('Issue key'), row[1].get('Summary'), row[1].get('Status'),
                                     row[1].get('Description'))
        print("Done")


if __name__ == '__main__':
    Jira2Trello().import_data()