import praw

reddit = praw.Reddit("bot1")

comments= []

for comment in reddit.redditor('Diet_Fanta').comments.new(limit=None):
    comments.append(comment.body)

counter = 0

for comment in comments:
    for word in comment.split(" "):
        if "teq" in word or "Teq" in word:
            counter += 1

print(counter)