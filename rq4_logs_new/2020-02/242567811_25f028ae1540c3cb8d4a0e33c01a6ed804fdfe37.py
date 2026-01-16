from github import Github

import json
import os
import pprint
import bugzilla

# public test instance of bugzilla.redhat.com. It's okay to make changes
BZ_URL = "https://bugzilla.redhat.com"

# First create a Github instance:

GH_USER = ""
GH_PASS = ""
GH_TOKEN = ""
GH_REPO = "amarts/glusterfs"
GH_LABELS = ["Migrated", "Type:Bug"]

def create_issue(repo_handle, title, desc, labels, assign=None):
    if not assign:
        return repo_handle.create_issue(title=title,
                                        body=desc,
                                        labels=labels)
    return  repo_handle.create_issue(title=title,
                                     body=desc,
                                     labels=labels,
                                     assignee=assign)
    

def main():
    # using username and password
    # gh = Github(GH_USER, GH_PASS)
    gh = Github(GH_TOKEN)
    repo = gh.get_repo(GH_REPO)
    bzapi = bugzilla.Bugzilla(BZ_URL)

    all_assignee_map = {}
    with open("assignee.list") as alist:
        email, gh_id = alist.readline().split(' ')
        all_assignee_map[email] = gh_id

    # for each bug
    bugId = 1193929
    bug = bzapi.getbug(bugId)
    print(bug)
    pprint.pformat(bug)
    assignee = all_assignee_map.get(bug.assigned_to, None)
    summary = "[bug:%d] %s" % (bugId, bug.summary)
    update = comments = bug.getcomments()
    body = "bugzilla-URL: %s/%s\n%s" % (BZ_URL, bugId, comments[0]['text'])
    print(comments[0])
    issue = create_issue(repo, summary, body,
                         GH_LABELS, assignee)
    pprint.pformat(comments)
    if len(comments) > 10:
        update = comments[-1]
    print(update)
    issue.create_comment(pprint.pformat(update))

    closing_msg = "This bug is moved to %s, and will be tracked there from now on. Visit GitHub issues URL for further details" % issue.url
    print(closing_msg)
    # TODO: uncomment it in last stage
    if not bzapi.logged_in:
        print("requires login credentials for %s, as we are closing bugs" % BZ_URL)
        #bzapi.interactive_login()

    # bug.close('UPSTREAM', comment=closing_msg)


main()