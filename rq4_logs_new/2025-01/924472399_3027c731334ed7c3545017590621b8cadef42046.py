import json
from datasets import load_dataset

ds = load_dataset("jamescalam/youtube-transcriptions")

# convert dataset to result.json
# the result.json should look like this:
# {
#   "video_id": {
#     "title": string,
#     "clusters": [
#       {
#         "comments": [
#           string,
#           ...
#         ]
#       }
#     ]
#   },
#   "video_id": {
#     "title": string,
#     "clusters": [
#       {
#         "comments": [
#           string,
#           ...
#         ]
#       }
#     ]
#   },
#   ...
# }
# where video_id is a column from the dataset object
# the string in "title" is a column from dataset object called "title"
# the string in "comments" is a column from dataset object called "text"
# "comments" will contain multiple string "text" with the same video_id

result = {}

for row in ds['train']:
    video_id = row['video_id']
    title = row['title']
    comment = row['text']

    if video_id not in result:
        result[video_id] = {
            "title": title,
            "clusters": [
                {
                    "comments": []
                }
            ]
        }

    result[video_id]["clusters"][0]["comments"].append(comment)

# randomly select 20% of the videos
result = {k: result[k] for k in list(result.keys())[:int(len(result) * .2)]}

with open('data/results.json', 'w') as f:
    json.dump(result, f, indent=4)