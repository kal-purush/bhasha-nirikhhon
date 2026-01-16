import praw
print("Initializing crawler...")
user_agent = "DB16app by DB16scraper, woo us!"
r = praw.Reddit(user_agent=user_agent)
subreddits = ["todayilearned", "science", "askscience","space","AskHistorians","YouShouldKnow","explainlikeimfive"]
print("Fetching subreddit posts...")
top_posts_per_subreddit = [ r.get_subreddit(x).get_hot(limit=5) for x in subreddits ]

for subreddit in top_posts_per_subreddit:
    print("Post:")
    posts = [str(post) for post in subreddit]
    print(posts)
    for submission in subreddit:
        print("Comments:")
        thread = next(submission)
        print(thread.comments)

    submissions = r.get_subreddit('python').get_top(limit=10)
    hannes = next(submissions)
    flat_comments = praw.helpers.flatten_tree(hannes.comments)
    for comment in flat_comments:
        print(comment)