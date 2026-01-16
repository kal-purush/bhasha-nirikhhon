"""
bins gold difference data

"""

import pandas as pd

# number of bins
bins = 10

# read in the other csv file
df = pd.read_csv("../data/GoldDiffOTBMwithFrameLen.csv")

# the columns we want
cols = ['gameID', 'winnerTotal', 'loserTotal', 'GoldDifference', 'bin']

# the new dataframe
output_df = pd.DataFrame(columns=cols)

# get rid of the outliers
# determined after looking at bar chart of values
print("-- removing outliers --")
df = df[df.framesLength < 43]
df = df[df.framesLength > 14]

# get each match id once
matches_list = list(df.gameID.unique())

# go throught matches one by one
for gameID in matches_list:
    # get a dataframe for just that match
    game_df = df[df.gameID == gameID]

    # calculate the number of frames that will go into every bin
    frames_per_bin = int(game_df.framesLength.mean() / bins)

    # then get the data for each bin
    for i in range(bins-1):
        # offset is the number of the frame to start from
        offset = i * frames_per_bin

        # calculate the number of frames to stop at
        read_until = (i + 1) * frames_per_bin

        # get those frames and drop the unneeded columns
        bin_df = game_df.iloc[offset:read_until, :] \
            .drop(['framesLength', 'timeStamp'], axis='columns')

        # group by the match id and get the average for the other columns
        new_df = bin_df.groupby(['gameID']).mean()

        # assign a bin label
        new_df['bin'] = i + 1

        # it thinks the match id is the index so undo that
        new_df.reset_index(level=0, inplace=True)

        # add it to our new dataframe that we'll print out
        output_df = output_df.append(new_df)

# write the data our to a CSV file
output_df.to_csv("../data/GoldDiffBinned.csv")


