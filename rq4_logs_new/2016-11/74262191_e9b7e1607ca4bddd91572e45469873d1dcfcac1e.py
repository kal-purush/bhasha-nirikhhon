#!/usr/bin/env python

from google.appengine.ext import db
import main


# Keys
def blog_key(name = 'default'):
    return db.Key.from_path('blogs', name)

def users_key(group = 'default'):
    return db.Key.from_path('users', group)

def comments_key(group = 'default'):
    return db.Key.from_path('comments', group)

def likes_key(group = 'default'):
    return db.Key.from_path('likes', group)

def dislikes_key(group = 'default'):
    return db.Key.from_path('dislikes', group)

# Database models
class BlogPosts(db.Model):
    """
    Database model for BlogPosts. Also contains many related functions
    that interact with other databases. Does the bulk of heavy lifting
    in terms of rendering blog posts, likes, and dislikes.
    """

    subject = db.StringProperty(required = True)
    content = db.TextProperty(required = True)
    created = db.DateTimeProperty(auto_now_add = True)
    modified = db.DateTimeProperty(auto_now = True)
    created_user_id = db.StringProperty(required = True)

    def render(self, user, is_static=False):
        """
        Renders the page. Passes through user and is_static for
        other renderers to use.
        """
        return main.render_str("post.html",
                          post = self,
                          user = user,
                          is_static=is_static)

    @classmethod
    def by_id(cls, post_id):
        """Returns the BlogPost entity with the given post_id"""
        return BlogPosts.get_by_id(int(post_id), parent = blog_key())

    @classmethod
    def get_post_creator_name(cls, user_id):
        """
        Returns the username of the given user_id that created the
        given post_id.
        """
        user = Users.by_id(int(user_id))
        return user.name

    @classmethod
    def get_post_comments(cls, post_id):
        """Returns the comments with the given post id."""
        return Comments.all().ancestor(comments_key()).filter(
            'related_post_id =', str(post_id))

    @classmethod
    def get_top_comments(cls, post_id):
        """Returns the top three comments for the given post_id."""
        return Comments.all().ancestor(comments_key()).filter(
            'related_post_id =', str(post_id)).run(limit=3)

    @classmethod
    def add_like_or_dislike(cls, like, dislike, post_id, user_id):
        """
        Takes the results of posting Like, Dislike, user_id, and
        post_id. Adds a like or dislike depending which exists in
        what's passed. Returns a dictionary composed of error and
        post id.
        """
        # Get associated post
        post = BlogPosts.by_id(int(post_id))
        error = {}

        if like:
            if post.has_user_liked_post(user_id, post_id):
                error["text"] = "You can't like a post twice."
                error["post"] = long(post_id)
            else:
                like = Likes(parent = likes_key(), 
                             post_id = post_id,
                             user_id = user_id)
                like.put()
            if post.has_user_disliked_post(user_id, post_id):
                has_disliked = post.get_dislike_by_user(user_id, post_id)
                has_disliked.delete()
        if dislike:
            if post.has_user_disliked_post(user_id, post_id):
                error["text"] = "You can't dislike a post twice."
                error["post"] = long(post_id)
            else:
                dislike = Dislikes(parent = dislikes_key(),
                                   post_id = post_id,
                                   user_id = user_id)
                dislike.put()
            if post.has_user_liked_post(user_id, post_id):
                has_liked = post.get_like_by_user(user_id, post_id)
                has_liked.delete()

        return error

    @classmethod
    def get_post_likes(cls, post_id):
        """
        Returns the filter for likes associated with the post. Use
        `.run()` on the result.
        """
        return Likes.all().ancestor(likes_key()).filter(
            'post_id =', str(post_id))

    @classmethod
    def get_post_dislikes(cls, post_id):
        """
        Returns the filter for dislikes associated with the post. Use
        `.run()` on the result.
        """
        return Dislikes.all().ancestor(dislikes_key()).filter(
            'post_id =', str(post_id))

    @classmethod
    def get_num_likes(cls, post_id):
        """Returns the number of likes."""
        likes = cls.get_post_likes(post_id)
        count = 0
        for like in likes:
            if str(like.post_id) == str(post_id):
                count += 1
        return count

    @classmethod
    def get_num_dislikes(cls, post_id):
        """Returns the number dislikes."""
        dislikes = cls.get_post_dislikes(post_id)
        count = 0
        for dislike in dislikes:
            if str(dislike.post_id) == str(post_id):
                count += 1
        return count

    @classmethod
    def has_user_liked_post(cls, user_id, post_id):
        """
        Returns True if the given user has created a like for the
        given post and False otherwise.
        """
        likes_with_post_id = cls.get_post_likes(post_id)
        has_liked = False
        for like in likes_with_post_id.run():
            if str(like.user_id) == str(user_id):
                has_liked = True
                break
        return has_liked

    @classmethod
    def has_user_disliked_post(cls, user_id, post_id):
        """
        Returns True if the given user has created a dislike the given
        post and False otherwise.
        """
        dislikes_with_post_id = cls.get_post_dislikes(post_id)
        has_disliked = False
        for dislike in dislikes_with_post_id.run():
            if str(dislike.user_id) == str(user_id):
                has_disliked = True
                break
        return has_disliked


    @classmethod
    def get_like_by_user(cls, user_id, post_id):
        """
        Returns a Likes entity with the given user_id for the
        given post_id.
        """
        likes = cls.get_post_likes(post_id).run()
        for like in likes:
            if str(like.user_id) == str(user_id):
                return like

    @classmethod
    def get_dislike_by_user(cls, user_id, post_id):
        """
        Returns a Dislikes entity with the given user_id for the
        given post_id.
        """
        dislikes = cls.get_post_dislikes(post_id).run()
        for dislike in dislikes:
            if str(dislike.user_id) == str(user_id):
                return dislike

    @classmethod
    def render_like(cls, post, disabled=False):
        """
        Renders the like for the post. If disabled is True, renders
        the disabled version.
        """
        return main.render_str('like.html', disabled = disabled, post = post)

    @classmethod
    def render_dislike(cls, post, disabled=False):
        """
        Renders the dislike for the post. If disabled is True, renders
        the disabled version.
        """
        return main.render_str('dislike.html', disabled = disabled, post = post)

class Comments(db.Model):
    """
    Database model for Comments. Also contains several related
    functions, some of which interact with other models.
    """
    subject = db.StringProperty(required = True)
    content = db.TextProperty(required = True)
    created_user_id = db.StringProperty(required = True)
    related_post_id = db.StringProperty(required = True)
    created = db.DateTimeProperty(auto_now_add = True)

    @classmethod
    def by_id(cls, comment_id):
        """Returns the Comments entity with the given comment_id."""
        return Comments.get_by_id(int(comment_id), parent = comments_key())

    @classmethod
    def add_comment(cls, subject, content, post_id, user_id):
        """Adds a comment for the given post."""
        if subject and content and post_id and user_id:
            comment = Comments(parent = comments_key(),
                               subject = subject,
                               content = content,
                               created_user_id = user_id,
                               related_post_id = post_id)
            comment.put()

    @classmethod
    def get_creator_username(cls, comment_id = ""):
        """
        Gets the creator username of the given comment id. If no
        comment ID is given, attempts to use the current comment's
        created_user_id.
        """
        if not comment_id:
            comment_id = cls.key().id()
        comment = cls.by_id(int(comment_id))
        user = Users.by_id(int(comment.created_user_id))
        return user.name

    @classmethod
    def render_comment(cls, comment_id, user, is_static=False):
        """
        Renders the given comment for the given post. If no comment ID
        is given, attempts to render the current comment. If small is
        True renders the condensed version for pages with multipl
        posts.
        """
        if not is_static:
            comment_renderer = "comment_sml.html"
        else:
            comment_renderer = "comment.html"
        return main.render_str(
            comment_renderer, comment = cls.by_id(int(comment_id)), user=user)

class Likes(db.Model):
    """
    Database model for Likes. Simply contains likes and is called
    by other classes.
    """
    post_id = db.StringProperty(required = True)
    user_id = db.StringProperty(required = True)

class Dislikes(db.Model):
    """
    Database model for Dislikes. Simply contains dislikes and is
    called by other classes.
    """
    post_id = db.StringProperty(required = True)
    user_id = db.StringProperty(required = True)

class Users(db.Model):
    """Users database model."""
    
    name = db.StringProperty(required = True)
    password = db.StringProperty(required = True)
    email = db.StringProperty()
    created = db.DateTimeProperty(auto_now_add = True)
    
    @classmethod
    def by_id(cls, uid):
        """Returns the user with the given id."""
        return Users.get_by_id(uid, parent = users_key())

    @classmethod
    def by_name(cls, name):
        """
        Returns the user with the given username. It's preferred to
        use id when possible.
        """
        user = Users.all().filter('name =', name).get()
        return user

    @classmethod
    def register(cls, name, password, email = None):
        """
        Calls registering the given data as a User entity. Still need
        to put() after calling.
        """
        return Users(parent = users_key(),
                     name = name,
                     password = password,
                     email = email)

    @classmethod
    def user_login(cls, name, password):
        """Validates user's password given the username."""
        logging.info('username = %s' % name)
        logging.info('password = %s' % password)
        user = cls.by_name(name)
        logging.info('user = %s' % type(user))
        if user and validate_password(name, password, user.password):
            return user

    @classmethod
    def user_id_str(cls, user_id):
        """
        Returns a string version of the user_id. Used for comparing
        strings in html.
        """
        return str(user_id)

    @classmethod
    def get_users_posts(cls, user_id):
        """Returns the posts of the given user_id."""
        users_posts = BlogPosts.all().ancestor(blog_key()).filter(
            "created_user_id =", str(user_id))
        if users_posts.get() != None:
            posts = users_posts.run()
        else:
            posts = None
        return posts

    @classmethod
    def render_post_editor(cls, subject = "", content = "", error = ""):
        """
        Renders the page editor. For new posts or comments and post
        or comment editing.
        """
        return main.render_str('post_editor.html',
                          subject = subject,
                          content = content,
                          error = error)