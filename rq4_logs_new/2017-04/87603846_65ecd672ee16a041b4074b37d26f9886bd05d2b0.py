import pandas as pd
import numpy as np

def text_df(df):
    df = df[df.is_retweet.isin([False])]
    df = df[['text']]
    return df.as_matrix()

# I considered excluding certain ways that tweets
# were made that were likely not to be written by
# Trump himself, but am not currently using it
def source_text_df(df):
    df = df[df.is_retweet.isin([False])]
    df = df[df.source.isin(['Twitter Web Client', 'TwitLonger Beta', 'TweetDeck', 'Mobile Web (M5)',
                            'Facebook',# 'Twitter for iPhone',
                            'Twitter for Android', 'Instagram',
                            'Twitlonger', 'Vine - Make a Scene', 'Twitter for BlackBerry',
                            'Twitter for Websites', 'Twitter Ads', 'Periscope', 'Twitter Mirror for iPad',
                            'Twitter QandA', 'Neatly For BlackBerry 10', 'Twitter for iPad',
                            'Media Studio'])]
    df = df[['text']]
    return df

df09 = pd.read_json('tweets/condensed_2009/condensed_2009.json')
df10 = pd.read_json('tweets/condensed_2010/condensed_2010.json')
df11 = pd.read_json('tweets/condensed_2011/condensed_2011.json')
df12 = pd.read_json('tweets/condensed_2012/condensed_2012.json')
df13 = pd.read_json('tweets/condensed_2013/condensed_2013.json')
df14 = pd.read_json('tweets/condensed_2014/condensed_2014.json')
df15 = pd.read_json('tweets/condensed_2015/condensed_2015.json')
df16 = pd.read_json('tweets/condensed_2016/condensed_2016.json')
df17 = pd.read_json('tweets/condensed_2017/condensed_2017.json')

df = pd.concat([df09,df10,df11,df12,df13,df14,df15,df16,df17],axis=0)

df_text = text_df(df)
print("text df {}".format(df_text.shape))
# source_df = source_text_df(df)
# print("android: {}".format(source_df.shape))

# print("source names: {}".format(pd.unique(df.source.ravel())))
np.save('tweet_text.npy',df_text)
# source_df.to_pickle('tweet_text_wo_iphone.pickle')
print(df_text)