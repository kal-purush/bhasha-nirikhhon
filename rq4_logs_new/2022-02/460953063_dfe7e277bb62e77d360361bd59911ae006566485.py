import os
import subprocess
import dxpy
import pandas as pd
from key import token
import re

dxpy.set_security_context({"auth_token_type": "Bearer", "auth_token": token})


def download_url(file_ID, project_ID):
    dxfile = dxpy.DXFile(file_ID)
    download_link = dxfile.get_download_url(
        duration=82800,
        preauthenticated=True,
        project=project_ID,
    )
    return download_link[0]


def find_data(filename):
    data = list(
        dxpy.bindings.search.find_data_objects(
            name=filename, name_mode="glob", describe=True, created_after="-1w"
        )
    )
    return data


def create_BAM_df(BAM_Data):
    data = []
    for object in BAM_Data:
        file_name = object.get("describe").get("name")
        folder = object.get("describe").get("folder")
        pattern = re.compile(r"(Pan\d+_\w\d+)")
        pan_number = pattern.search(file_name)
        try:
            pan_num = pan_number.group()
        except:
            pattern = re.compile(r"(Pan\d+)")
            pan_number = pattern.search(file_name)
            pan_num = pan_number.group()
        BAI_name = file_name + ".bai"
        object_id = object.get("describe").get("id")
        project_id = object.get("describe").get("project")
        merged_data = [BAI_name, folder, pan_num, project_id, object_id]
        data.append(merged_data)
    return pd.DataFrame(
        data, columns=["name", "folder", "pan_num", "project_id", "bam_file_id"]
    )


def create_BAI_df(Index_Data):
    data = []
    for object in Index_Data:
        file_name = object.get("describe").get("name")
        folder = object.get("describe").get("folder")
        object_id = object.get("describe").get("id")
        project_id = object.get("describe").get("project")
        merged_data = [file_name, folder, project_id, object_id]
        data.append(merged_data)
    return pd.DataFrame(data, columns=["name", "folder", "project_id", "index_file_id"])


def final_url_links(merged_df):
    merged_df = merged_df.sort_values("project_id")
    # create new columns with url links and project name
    merged_df["project_name"] = ""
    merged_df["url"] = ""
    merged_df["indexURL"] = ""
    # fill first row with required data
    bam_id = merged_df["bam_file_id"][0]
    index_id = merged_df["index_file_id"][0]
    project_id = merged_df["project_id"][0]
    project_data = dxpy.bindings.dxproject.DXProject(dxid=project_id)
    merged_df["project_name"] = project_data.describe().get("name")
    bai_name = merged_df["name"][0]
    pattern = re.compile(r"(\S+.bam)")
    pattern_search = pattern.search(bai_name)
    bam_name = pattern_search.group()
    merged_df["url"][0] = download_url(bam_id, project_id) + "/" + bam_name
    merged_df["indexURL"][0] = download_url(index_id, project_id) + "/" + bai_name
    # loop to fill the rest of the rows with the required data
    for i in range(1, len(merged_df)):
        bai_name = merged_df["name"][i]
        pattern_search = pattern.search(bai_name)
        bam_name = pattern_search.group()
        project_id = merged_df["project_id"][i]
        prev_project_id = merged_df["project_id"][i - 1]
        bam_id = merged_df["bam_file_id"][i]
        index_id = merged_df["index_file_id"][i]
        if project_id == prev_project_id:
            merged_df["project_name"][i] = merged_df["project_name"][i - 1]
        else:
            project_data = dxpy.bindings.dxproject.DXProject(dxid=project_id)
            merged_df["project_name"][i] = project_data.describe().get("name")
        merged_df["url"][i] = download_url(bam_id, project_id) + "/" + bam_name
        merged_df["indexURL"][i] = download_url(index_id, project_id) + "/" + bai_name
    return merged_df.sort_values(["name", "folder"])


# Retrieve infomration for BAM files
all_BAM = find_data("*.bam")
all_BAM_df = create_BAM_df(all_BAM)

# Retrieve information for index files
all_BAI = find_data("*.bam.bai")
all_BAI_df = create_BAI_df(all_BAI)

# merged two dataframes with bam and index ids using three keys:
merged = pd.merge(all_BAM_df, all_BAI_df, on=["name", "folder", "project_id"])

# generate the url links and project name
url_links = final_url_links(merged)
url_links.to_csv("hg19_dnanexus.csv", index=False, sep=",")