import praw
import requests
import random

reddit = praw.Reddit(client_id="M87nRI3kCjaYRw", client_secret="2-sFdzE_TE58cQTMZwLpxZNiC60", user_agent='Web app created for Hack the North 2018')


def emotion_to_image(emotion):
    emotions = {'anger': None, 'contempt': None, 'disgust': None, 'fear': 'getmotivated', 'happiness': None, 'neutral': None, 'sadness': 'wholesomememes', 'surprise': None, }

    submissions = []

    for post in reddit.subreddit(emotions[emotion]).top("week", limit=10):
        submissions.append(post.url)
    return submissions[random.randint(0, 10)]

if __name__ == "__main__":
    print(emotion_to_image('sadness'))