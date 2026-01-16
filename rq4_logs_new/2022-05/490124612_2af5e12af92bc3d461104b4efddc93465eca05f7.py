import tweepy
Api_keys = 'whQ5aXoe0f7OnBqRYxplsDvnz'
API_secret_key = 'LxiuUTPJtSGRUQhVLwlyKgidmGs4OSUmGHFn8qHrV7KG39fqBq'
Bearer_token = 'AAAAAAAAAAAAAAAAAAAAACeKcQEAAAAAy35MC2dyEd43PR9lFNBARgZatW8%3DOfROunkFK9MH0hnxKZoQUQlNns9X1Un5y8Gbc7NLUBnUdiVIHU'
Access_token = '1523482134853435392-T3YcU175wgdUnlNufspbO16cAcKaNA'
access_secret = 'liF9rIjRzgRQxF4g3KLnOVvDb4Jpt5VsIPHggM7MWvFNs'

auth = tweepy.OAuth1UserHandler(Api_keys, API_secret_key)
auth.set_access_token(Access_token, access_secret)
api = tweepy.API(auth)
try:
    api.verify_credentials()
    print('Works!!')
except:
    print('Problem')
#timeline = api.home_timeline()
user = api.get_user(screen_name="Davis_Johnson1")
api.create_friendship(user_id=user.id)
Austin_timeline = api.user_timeline(user_id=user.id, count=200, include_rts=False, tweet_mode='extended')
for tweet in Austin_timeline:
    api.create_favorite(tweet.id)
    api.retweet(tweet.id)