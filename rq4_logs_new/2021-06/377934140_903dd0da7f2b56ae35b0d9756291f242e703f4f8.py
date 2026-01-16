# more imports than we need
import geopandas as gpd
import maup
import pandas as pd
import matplotlib.pyplot as plt
import requests
import json
import csv
import io
import pydantic
from pydantic import BaseModel
from typing import Tuple

class Submission(pydantic.BaseModel):
    """
    pydantic class containing districtr link, districtr plan (assignment),  ...
    the type of the plan 
    """
    link: str
    districtr_plan: dict # dict: districtr json obj
    plan_type: str
    id: str

def submissions(ids_url: str, plans_url: str, cois_url: str,
                wr_url: str) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Takes in endpoint for all districtr ids in a portal along with csv api  ...
    calls for plans, cois, and written submissions, and retrieves filled pd ...
    dataframes for each submission type with metadata and districtr assignments
    """
    submissions = retrieve_submission_ids_json(ids_url)
    submissions.sort(key=lambda x: str(x.id)) # sorts submission jsons by id
    plan_submissions = [sub.districtr_plan for sub in submissions #filters plan
                                                    if sub.plan_type == "plan"]
    coi_submissions = [sub.districtr_plan for sub in submissions #filters cois
                                                    if sub.plan_type == "coi"]
    plan_submissions_df = csv_read(plans_url) # gathers plan metadata in df
    coi_submissions_df = csv_read(cois_url) # gathers coi metadata in df
    written_submissions_df = csv_read(wr_url) # gathers written metadata in df
    assert len(plan_submissions) == len(plan_submissions_df)
    assert len(coi_submissions) == len(coi_submissions_df)
    # parse for plan id and add in submission dfs
    plan_submissions_df['plan_id'] = plan_submissions_df["link"].map(
                                lambda link: link.split("/")[-1].split("?")[0])
    coi_submissions_df['plan_id'] = coi_submissions_df["link"].map(
                                lambda link: link.split("/")[-1].split("?")[0])
    # sort dfs by plan id to correctly join w/ json information
    plan_submissions_df = plan_submissions_df.sort_values(
                                                by=['plan_id'], ascending=True)
    coi_submissions_df = coi_submissions_df.sort_values(
                                                by=['plan_id'], ascending=True)
    # join in districtr json assignments into 'districtr_data column'
    plan_submissions_df['districtr_data'] = plan_submissions
    coi_submissions_df['districtr_data'] = coi_submissions
    # return relevant dataframes
    return plan_submissions_df, coi_submissions_df, written_submissions_df

def plan_read(plan_id: int) -> dict: #(dict: json obj)#
    """
    takes in plan_id string, makes api call w/ plan_id to the planRead funct...
    in netlify, and returns the data associated with the plan_id in JSON format
    """
    url = "https://districtr.org/.netlify/functions/planRead?id=%s" % plan_id
    r = requests.get(url)
    data = json.loads(r.text)
    return data

def retrieve_submission_ids_json(url: str) -> list: #list: list[Submission]
    """
    retrieveSubmissionJson takes a url (an endpoint to a given state's...
    submission portal), returns a list of filled Submission objects
    """
    r = requests.get(url)
    subs_json = json.loads(r.text)
    submissions = []
    for ids in subs_json['ids']:
        # Phase 1, retrieve link, id, type of plan
        plan_link = ids['link']
        plan_id = plan_link.split("/")[-1].split("?")[0]
        plan_type = ids['type']
        # Phase 2, fill submission with phase 1 + the ditrictr plan(assignment)
        submissions.append(
                Submission(link=plan_link, plan_type=plan_type,
                           id=plan_id, districtr_plan=plan_read(plan_id)))
    return submissions

def csv_read(url: str) -> pd.DataFrame:
    """
    takes in a url (api endpt to query on given submission portal) to find  ...
    in csv form data from the portal, and returns a pandas dataframe filled ...
    with the portal info
    """
    # TODO: temp fix for the purposes of user-agent api call barrier
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.101 Safari/537.36'}  
    r = requests.get(url, headers=headers).content
    read_file = pd.read_csv(io.StringIO(r.decode('utf-8')))
    return read_file