from friendships.services import FriendshipService
from newsfeeds.models import NewsFeed
from utils.time_constants import ONE_HOUR
from celery import shared_task
from tweets.models import Tweet


@shared_task(time_limit=ONE_HOUR)
def fanout_to_followers_task(tweet_id):
    from newsfeeds.services import NewsFeedService

    tweet = Tweet.objects.get(id=tweet_id)
    # get all followers of the poster
    followers = FriendshipService().get_followers(tweet.user)
    # create newsfeed for all followers
    newsfeeds = [
        NewsFeed(user=follower, tweet=tweet)
        for follower in followers
    ]
    # the poster can see own posted tweet
    newsfeeds.append(NewsFeed(user=tweet.user, tweet=tweet))
    # create objects by batch, more efficiency
    NewsFeed.objects.bulk_create(newsfeeds)

    # bulk_create() doesn't trigger post_save signal
    # trigger the post_save manually in iteration
    for newsfeed in newsfeeds:
        NewsFeedService.push_newsfeed_to_cache(newsfeed)