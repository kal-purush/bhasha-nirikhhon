from pymongo import MongoClient
client = MongoClient()
db = client.fb
pagesCollection = db.pages
postsCollection = db.posts
commentsCollection = db.comments
likesCollection=db.likes


def countCollections():
    print '%d pages' % pagesCollection.count()
    print '%d posts' % postsCollection.count()
    print '%d comments' % commentsCollection.count()
    print '%d likes' % likesCollection.count()