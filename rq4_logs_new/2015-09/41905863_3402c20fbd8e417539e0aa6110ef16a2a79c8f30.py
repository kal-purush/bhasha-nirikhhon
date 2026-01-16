from github import Github
from configparser import ConfigParser
from argparse import ArgumentParser
import os
import xdg.BaseDirectory

# Resolve the XDG config path
config_dir = xdg.BaseDirectory.save_config_path('github-changelog')
config_file = os.path.join(config_dir, "config.ini")

# First time setup
if not os.path.isfile(config_file):
    print("No configuration found. Starting initial setup.")
    print("Github-changelog needs a token to communicate with github.")
    print("Please create a token on https://github.com/settings/tokens")
    token = input("Your personal access token: ")
    config = ConfigParser()
    config.add_section("general")
    config.set("general", "token", token)
    with open(config_file, "w") as config_handle:
        config.write(config_handle)

# Initialize Github connection
config = ConfigParser()
config.read(config_file)
token = config['general']['token']
github = Github(token)

# Parse the command line arguments
argparse = ArgumentParser(description="Changelog generator for Github")
argparse.add_argument("-f", "--format", help="Output format", default="simple", action="store",
                      choices=["simple", "table"])
argparse.add_argument("-g", "--group-by", help="Group output by github labels, use multiple times for every label",
                      action="append")
argparse.add_argument("repository", help="The github repository in user/repo format")
argparse.add_argument("milestone", help="The milestone to create a changelog for", action="store")

args = argparse.parse_args()

# Resolve the milestone argument to a milestone object on github
repository = github.get_repo(args.repository)
milestone = None
for m in repository.get_milestones():
    if m.title == args.milestone:
        milestone = m

if not milestone:
    print("Cannot find milestone {} in repository {}".format(args.milestone, args.repository))
    exit(1)

# Set up grouping
groups = {}
grouping = False
if args.group_by:
    grouping = True
    for group in args.group_by:
        groups[group] = []
    groups["Other"] = []
else:
    groups["Issues"] = []

# Retrieve and group all github issues for the milestone
issues = repository.get_issues(milestone=milestone, state="all")
for issue in issues:
    if not grouping:
        groups["Issues"].append(issue)
        continue

    labels_github = issue.labels
    labels = []
    for l in labels_github:
        labels.append(l.name)
    intersect = list(set(args.group_by) & set(labels))
    if len(intersect) == 0:
        intersect = ["Other"]
    for label in intersect:
        groups[label].append(issue)

# Remove empty groups

for group in list(groups.keys()):
    if len(groups[group]) == 0:
        groups.pop(group)

# Feed the grouped issues to a formatter
if args.format == "simple":
    """
    ## Group name

    The issue title #1
    Another issue #2
    """
    for group in groups:
        print("## {}\n".format(group))
        for issue in groups[group]:
            print("{} #{}".format(issue.title, issue.number))
        print("")

if args.format == "table":
    """
    ## Group name

    | # | Title | Labels |
    | - | ----- | ------ |
    | 1 | The.. | bla bl |
    """
    for group in groups:
        print("## {}\n".format(group))
        print("| # | Title | Labels |")
        print("| - | ----- | ------ |")
        for issue in groups[group]:
            labels = []
            for l in issue.labels:
                labels.append(l.name)
            labels = ", ".join(labels)
            print("| #{} | {} | {} |".format(issue.number, issue.title, labels))
        print("")