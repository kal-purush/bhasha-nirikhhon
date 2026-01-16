import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

from utils import save_fig

CHAPTER_ID = "kaggle"
doplot = False

## URL : https://www.kaggle.com/ankkur13/sentiment-analysis-nlp-wordcloud-textblob/notebook

df_usa = pd.read_csv("./input/youtube-new/USvideos.csv")
df_ca = pd.read_csv("./input/youtube-new/CAvideos.csv")
df_de = pd.read_csv("./input/youtube-new/DEvideos.csv")
df_fr = pd.read_csv("./input/youtube-new/FRvideos.csv")
df_gb = pd.read_csv("./input/youtube-new/GBvideos.csv")

# In the dataset, the Trending Date and Published Time are not in the Unix date-time format. Let's fix this first.

df_usa["trending_date"] = pd.to_datetime(df_usa["trending_date"], format='%y.%d.%m')
df_usa["publish_time"] = pd.to_datetime(df_usa["publish_time"], format='%Y-%m-%dT%H:%M:%S.%fZ')

# separates date and time into two columns from `publish_time` column

df_usa.insert(4, 'publish_date', df_usa["publish_time"].dt.date)
df_usa['publish_time'] = df_usa['publish_time'].dt.time
df_usa['publish_date'] = pd.to_datetime(df_usa['publish_date'])

if doplot:
# To see the correlation between the likes, dislikes, comments, and views lets plot a correlation matrix

    columns_show = ['views', 'likes', 'dislikes', 'comment_count']
    f, ax = plt.subplots(figsize=(8,8))
    corr = df_usa[columns_show].corr()
    sns.heatmap(corr, mask=np.zeros_like(corr, dtype=np.bool),
                cmap=sns.diverging_palette(h_neg=220, h_pos=10, as_cmap=True),
                square=True, ax=ax, annot=True)
    save_fig("yt_sentiment_analysis", CHAPTER_ID)

'''
Since, a video could be in trending for several days. There might be multiple rows of a particular video. 
In order to calculate the total Views, Comments, Likes, Dislikes of a video, we need to groupby with 
video_id. The below script will give you the total no. of views/comments/likes, and dislikes of a video.
'''

usa_videos_views = df_usa.groupby(['video_id'])['views'].agg('sum')
usa_videos_likes = df_usa.groupby(['video_id'])['likes'].agg('sum')
usa_videos_dislikes = df_usa.groupby(['video_id'])['dislikes'].agg('sum')
usa_videos_comment_count = df_usa.groupby(['video_id'])['comment_count'].agg('sum')

'''
To get the numbers of videos on which the 'Comments Disabled/ Rating Disabled/Video Error'. 
We need to remove the duplicates to get the correct numbers otherwise there will be redundancy.
'''

df_usa_single_day_trend = df_usa.drop_duplicates(subset='video_id', keep=False, inplace=False)
df_usa_multiple_day_trend = df_usa.drop_duplicates(subset='video_id', keep='first', inplace=False)

# print(df_usa_multiple_day_trend.head(n=4))

'''
Which video trended on maximum days and what is the title, likes, dislikes, comments, and views.
'''
df_usa_which_video_trended_maximum_days=df_usa.groupby(by=['video_id'],as_index=False).count().sort_values(by='title',ascending=False).head()

if doplot:
    plt.figure(figsize=(10,10))
    sns.set_style("whitegrid")
    ax = sns.barplot(x=df_usa_which_video_trended_maximum_days['video_id'],y=df_usa_which_video_trended_maximum_days['trending_date'], data=df_usa_which_video_trended_maximum_days)
    plt.xlabel("Video Id")
    plt.ylabel("Count")
    plt.title("Top 5 Videos that trended maximum days in USA")
    save_fig("top_5_videos_trending_for_maximum_days", CHAPTER_ID)
    plt.show()