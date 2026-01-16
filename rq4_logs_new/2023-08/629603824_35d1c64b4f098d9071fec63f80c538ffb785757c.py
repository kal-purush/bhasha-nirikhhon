# %%
import json
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

import os
if os.getcwd().split("/")[-1] == "scripts" or os.getcwd().split("/")[-1] == "notebooks":
    os.chdir("../")
print(f"Working in {os.getcwd()}")

with open("../workflow/config.json", "r") as cf:
    config = json.loads(cf.read())

groups = ["public", "congress", "journalists", "trump"]
frame_types = ["generic", "specific"]
frames = {k: {} for k in groups}
for group in ["public"]:
    for frame_type in frame_types:

        # all frames are in one file for the public
        # we have to merge a couple of different files in either case
        if group != "public":
            # predicted frames
            predictions = pd.read_csv(f"data/binary_frames/{group}/{group}_{frame_type}.tsv",
                                      sep="\t").drop("text", axis="columns")

            # time stamps for granger causality
            tweet_info = pd.read_csv(f"data/immigration_tweets/{group}.tsv",
                                     sep="\t")[["id_str", "time_stamp"]]

            tweet_info["id_str"] = tweet_info["id_str"].astype(str)
            predictions["id_str"] = predictions["id_str"].astype(str)

            # merge and add to list of dataframes to combine
            tweet_df = pd.merge(tweet_info, predictions,
                                how="right", on="id_str")
            frames[group][frame_type] = tweet_df
        else:
            # frame predictions
            predictions = pd.read_csv(f"data/binary_frames/predicted_frames.tsv",
                                      sep="\t")

            tweet_info = pd.read_csv("data/us_public_ids.tsv", sep="\t")
            tweet_info["time_stamp"] = pd.to_datetime(tweet_info["time_stamp"])
            
            # tweet_info["time_stamp"] = datetime.strptime(tweet_info["time_stamp"],
            #                                              "%a %b %d %H:%M:%S +0000 %Y")
            
            frames["public"] = pd.merge(tweet_info, predictions, on="id_str")



# %%