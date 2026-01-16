#!/usr/bin/env python
#
# Copyright 2007 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import os
import jinja2
import webapp2
import re
import random
import string
import hashlib
import hmac
import logging

from google.appengine.ext import db

template_dir = os.path.join(os.path.dirname(__file__), 'templates')
jinja_env = jinja2.Environment(loader = jinja2.FileSystemLoader(template_dir),
                               autoescape=True)

secret = "UElMsOPfxYrvaz2w"
BASE_LIKES = 0

def log(str):
    logging.info(str)

# Helper functions
def render_str(template, **params):
    t = jinja_env.get_template(template)
    return t.render(params)

def make_salt(length = 5):
    return "".join(random.choice(string.letters) for x in xrange(length))

def make_secure_val(val):
    return '%s|%s' % (val, hmac.new(secret, val).hexdigest())

def check_secure_val(secure_val):
    val = secure_val.split('|')[0]
    if secure_val == make_secure_val(val):
        return val

# Deprecated cookie functions
def make_user_id_hash(user_id, salt=None):
    if not salt:
        salt = make_salt()
    hash = hashlib.sha256(str(user_id) + salt).hexdigest()
    return '%s|%s' % (hash, salt)

def validate_user_id(user_id, hash):
    salt = hash.split('|')[-1]
    return hash == make_user_id_hash(user_id, salt)

def render_post(response, post):
    response.out.write('<b>' + post.subject + '</b><br>')

USER_RE = re.compile(r"^[a-zA-Z0-9_-]{3,20}$")
def is_valid_username(username):
    """Returns True if the username is valid and False otherwise."""
    return username and USER_RE.match(username)

def get_username_error(username):
    """
    If the username is invalid, returns an error related to how it's
    invalid. Otherwise, does nothing.
    """
    if not username:
        return "Please enter a username."
    elif not USER_RE.match(username):
        return ("Please enter a username of between 3 to 20 alphanumeric"
                " characters.")

PASS_RE = re.compile(r"^.{3,20}$")
def is_valid_password(password):
    return password and PASS_RE.match(password)

def get_password_error(password):
    """
    If the password is invalid, returns an error related to how it's
    invalid. Otherwise, does nothing.
    """
    if not password:
        return "Please enter a password."
    elif not PASS_RE.match(password):
        return "Please enter a password of between 3 to 20 characters."

EMAIL_RE = re.compile(r"^[\S]+@[\S]+.[\S]+$")
def is_valid_email(email):
    return not email or EMAIL_RE.match(email)


# User functions
def make_pw_hash(username, password, salt=None):
    if not salt:
        salt = make_salt()
    hsh = hashlib.sha256(username + password + salt).hexdigest()
    return '%s|%s' % (hsh, salt)

def validate_password(username, password, hsh):
    salt = hsh.split('|')[1]
    return hsh == make_pw_hash(username, password, salt)


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
        return render_str("post.html",
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
        return render_str('like.html', disabled = disabled, post = post)

    @classmethod
    def render_dislike(cls, post, disabled=False):
        """
        Renders the dislike for the post. If disabled is True, renders
        the disabled version.
        """
        return render_str('dislike.html', disabled = disabled, post = post)

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
        return render_str(
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
        return render_str('post_editor.html',
                          subject = subject,
                          content = content,
                          error = error)


# Page handlers
class Handler(webapp2.RequestHandler):
    """Base Handler class. Has writing and rendering basics."""
    def write(self, *a, **kw):
        self.response.out.write(*a, **kw)

    def render_str(self, template, **params):
        t = jinja_env.get_template(template)
        return t.render(params)

    def render(self, template, **kw):
        self.write(self.render_str(template, **kw))

class BlogHandler(Handler):
    """
    Base class for handling requests. Contains functions that would
    otherwise be duplicated in individual handlers.
    """
    def set_secure_cookie(self, name, val):
        """Adds the cookie with the given name and value."""
        cookie_val = make_secure_val(val)
        self.response.headers.add_header('Set-Cookie',
                                         '%s=%s; Path=/' % (name, cookie_val))

    def read_secure_cookie(self, name):
        """Validates the cookie and returns it's value."""
        cookie_val = self.request.cookies.get(name)
        return cookie_val and check_secure_val(cookie_val)

    def page_login(self, user):
        """Sets the user's cookie."""
        self.set_secure_cookie('user_id', str(user.key().id()))

    def logout(self):
        """Removes the users's cookie."""
        self.response.headers.add_header('Set-Cookie', 'user_id=; Path=/')

    def get_previous_page(self):
        """
        Checks if there's a previous page cookie set. If so, returns
        it. Otherwise, returns an empty string.
        """
        previous_page = self.read_secure_cookie("page")

        if not previous_page:
            previous_page = ""

        return previous_page

    def edit_post_redirect(self,
                           edit_post,
                           delete_post,
                           add_comment,
                           page,
                           post_id):
        """
        If edit_post, delete_post, or add_comment exist, will redirect
        the user to those pages while setting the requisite cookies.
        Otherwise, does nothing.
        """

        if edit_post or delete_post or add_comment:
            self.set_secure_cookie('post_id', str(post_id))
            self.set_secure_cookie('page', page)
            if edit_post:
                self.redirect('/editpost')
            if delete_post:
                self.redirect('/deletepost')
            if add_comment:
                self.redirect('/newcomment')
        else:
            pass

    def edit_comment_redirect(self,
                              edit_comment,
                              delete_comment,
                              comment_id,
                              page):
        """
        If edit_comment or delete_comment exist, will direct the user
        to those pages while setting the requisite cookies.
        Otherwise, does nothing.
        """
        if edit_comment or delete_comment:
            self.set_secure_cookie('comment_id', str(comment_id))
            self.set_secure_cookie('page', page)
            if edit_comment:
                self.redirect('/editcomment')
            if delete_comment:
                self.redirect('/deletecomment')
        else:
            pass

    def get_post_data(self):
        """Gets the data in the post needed for"""

        data = {
            "like" : self.request.get("Likes"),
            "dislike" : self.request.get("Dislikes"),
            "post_id" : self.request.get("post_id"),
            "user_id" : self.request.get("user_id"),
            "edit_post" : self.request.get("edit_post"),
            "delete_post" : self.request.get("delete_post"),
            "add_comment" : self.request.get("add_comment"),
            "edit_comment" : self.request.get("edit_comment"),
            "delete_comment" : self.request.get("delete_comment")
        }

        return data

    def initialize(self, *args, **kwargs):
        """If user is logged in allows the handler to call the user."""
        webapp2.RequestHandler.initialize(self, *args, **kwargs)
        uid = self.read_secure_cookie('user_id')
        self.user = uid and Users.by_id(int(uid))

class BlogFrontPageHandler(BlogHandler):
    """Handler for './blog'. """
    def render_front(self, error=""):
        """Gets the 10 most recent posts and sends them blog_front."""
        posts = BlogPosts.all().ancestor(blog_key()).order('-created').fetch(
            limit=10)

        self.render("blog_front.html",
                    user=self.user,
                    posts=posts,
                    error=error)

    def get(self):
        """Called when the page loads. Calls render_front."""
        self.render_front()

    def post(self):
        """
        Gets the data sent by the f/e, then checks whether to add
        likes/dislikes, edit/ delete the post, add/ edit/ delete
        comments.
        """
        data = self.get_post_data()

        error = {}

        if data["like"] or data["dislike"]:
            error = BlogPosts.add_like_or_dislike(like=data["like"],
                                                  dislike=data["dislike"],
                                                  post_id=data["post_id"],
                                                  user_id=data["user_id"])
            self.render_front(error)
        elif data["edit_post"] or data["delete_post"] or data["add_comment"]:
            self.edit_post_redirect(edit_post=data["edit_post"],
                                    delete_post=data["delete_post"],
                                    add_comment=data["add_comment"],
                                    page="blog",
                                    post_id=data["post_id"])
        elif data["edit_comment"] or data["delete_comment"]:
            comment_id = self.request.get("comment_id")
            self.edit_comment_redirect(edit_comment=data["edit_comment"],
                                       delete_comment=data["delete_comment"],
                                       comment_id=comment_id,
                                       page="blog")

class NewPostPageHandler(BlogHandler):
    """Handler for './newpost'."""
    def render_new_post(self, subject="", content="", error=""):
        """Renders the new post page."""
        self.render("new_post.html", 
                    subject=subject, 
                    content=content, 
                    error=error,
                    user=self.user)

    def get(self):
        """
        If the user is logged in renders the new post page. Otherwise
        redirects the user to sign up.
        """
        if self.user:
            self.render_new_post()
        else:
            self.redirect('/signup')

    def post(self):
        """
        Takes the subject and content strings. If there's data in
        both, creates the post and navigates to the static post page.
        Otherwise, renders the page with the error.
        """
        subject = self.request.get("subject")
        content = self.request.get("content")

        if subject and content:
            post = BlogPosts(parent = blog_key(),
                             subject = subject,
                             content = content,
                             created_user_id = str(self.user.key().id()))
            post.put()

            self.redirect("/%s" % post.key().id())
        else:
            error = "We need both a subject and some content!"
            self.render_new_post(subject, content, error)

class StaticPostPageHandler(BlogHandler):
    """Handler for individual post pages."""
    def render_static_page(self, post_id, error=""):
        """
        Checks that the post exists. If it does, renders the page.
        If the page does not renders the page with a simple 404
        message.
        """
        post = BlogPosts.by_id(int(post_id))
        if post == None:
            error = {
                "text" : "Sorry, we can't find that post.",
                "code" : "404"
            }
        self.render('static_post.html',
                    post=post,
                    user=self.user,
                    error=error,
                    is_static=True)

    def get(self, post_id):
        """Renders static page on page load."""
        self.render_static_page(post_id=post_id)

    def post(self, post_id):
        """
        Gets the posted data, then check whether the to add a like/
        dislike, edit/ delete the post, or add/ edit/ delete a comment.
        """
        data = self.get_post_data()

        error = {}

        if data["like"] or data["dislike"]:
            error = BlogPosts.add_like_or_dislike(like=data["like"],
                                                  dislike=data["dislike"],
                                                  post_id=data["post_id"],
                                                  user_id=data["user_id"])
            self.render_static_page(post_id=data["post_id"], error = error)
        elif data["edit_post"] or data["delete_post"] or data["add_comment"]:
            self.edit_post_redirect(edit_post=data["edit_post"],
                                    delete_post=data["delete_post"],
                                    add_comment=data["add_comment"],
                                    page=str(data["post_id"]),
                                    post_id=data["post_id"])
        elif data["edit_comment"] or data["delete_comment"]:
            comment_id = self.request.get("comment_id")
            self.edit_comment_redirect(edit_comment=data["edit_comment"],
                                       delete_comment=data["delete_comment"],
                                       comment_id=comment_id,
                                       page=str(data["post_id"]))

class UserSignUpHandler(BlogHandler):
    """Handler for './signup'."""
    def get(self):
        """
        Checks if the user is logged in. If so, redirects to the
        welcome page. Otherwise, renders the signup form.
        """
        if self.user:
            self.redirect('/welcome')
        else:
            self.render('signup.html')

    def post(self):
        """
        Gets all the users. Then validates the inputs. If they're
        valid, create the user and redirect the user to the welcome
        page. Otherwise, render the page with errors.
        """
        # Create users db
        users = db.GqlQuery("SELECT * FROM Users "
                             "ORDER BY created DESC ")

        # Get user info
        self.username = self.request.get('username')
        self.password = self.request.get('password')
        self.verify_password = self.request.get('verify')
        self.email = self.request.get('email')
        are_errors = False

        params = dict(username = self.username,
                      email = self.email)

        # Check the user's input
        if not is_valid_username(self.username):
            params['username_error'] = get_username_error(self.username)
            are_errors = True
            
        if not is_valid_password(self.password):
            params['password_error'] = get_password_error(self.password)
            are_errors = True
        elif self.password != self.verify_password:
            params['verify_password_error'] = (
                "Please enter a matching password.")
            are_errors = True
        else:
            # hash the password as soon as we decide it's valid
            self.hashed_password = make_pw_hash(self.username, self.password)

        if self.email:
            if not is_valid_email(self.email):
                params['email_error'] = "That's not a valid email."
                are_errors = True

        if are_errors:
            self.render('signup.html', **params)
        else:
            # Check if the username entered already exists
            user = Users.by_name(self.username)
            if user:
                params['username_error'] = "That user already exists."
                self.render('signup.html', **params)
            else:
                # Create the user in the db
                user = Users.register(self.username,
                                      self.hashed_password,
                                      self.email)
                user.put()

                self.page_login(user)
                self.redirect('/welcome')

class WelcomePageHandler(BlogHandler):
    """Handler for './welcome'."""
    def get(self):
        """
        Checks whether the user is logged in. If so, renders the
        welcome page. If not, redirects the user to the signup page.
        """
        if self.user:
            self.render('welcome.html', user = self.user)
        else:
            self.redirect('/signup')

class MyPostsPageHandler(BlogHandler):
    """Handler for './myposts'."""
    def render_my_posts(self, error = ""):
        """Gets the users posts, then renders the my posts page."""
        user = self.user
        posts = self.user.get_users_posts(self.user.key().id())
        self.render('my_posts.html', user = user, posts = posts, error = error)

    def get(self):
        """
        Checks if the user is logged in. If so, renders the my posts
        page. Otherwise, redirects to the signup page.
        """
        if self.user:
            self.render_my_posts()
        else:
            self.redirect('/signup')

    def post(self):
        """
        Gets the posted data, then checks whether to edit/delete a
        post, or add/ edit/ delete a comment.
        """
        data = self.get_post_data()
        
        if data["edit_post"] or data["delete_post"] or data["add_comment"]:
            self.edit_post_redirect(edit_post=data["edit_post"],
                                    delete_post=data["delete_post"],
                                    add_comment=data["add_comment"],
                                    page="myposts",
                                    post_id=data["post_id"])
        elif data["edit_comment"] or data["delete_comment"]:
            comment_id = self.request.get("comment_id")
            self.edit_comment_redirect(edit_comment=data["edit_comment"],
                                       delete_comment=data["delete_comment"],
                                       comment_id=comment_id,
                                       page="myposts")

class DeletePostPage(BlogHandler):
    """Handler for './deletepost'."""

    def render_delete_post_page(self):
        """
        Renders the delete post page.

        Checks if the post exists. If it does, checks if the user
        created the post. If so, renders the delete post page.
        Otherwise, redirects to the previous page.
        """
        post_id = self.read_secure_cookie("post_id")
        previous_page = self.get_previous_page()

        if post_id:
            post = BlogPosts.by_id(int(post_id))
            if not str(self.user.key().id()) == str(post.created_user_id):
                self.redirect('/')
            self.render('deletepost.html', user = self.user, post_id = post_id)
        else:
            self.redirect('/%s' % previous_page)

    def get(self):
        """
        Checks if the user is logged in. If so, renders the delete
        post page. Otherwise, redirects the user to signup page.
        """
        if self.user:
            self.render_delete_post_page()
        else:
            self.redirect('/signup')

    def post(self):
        """
        Gets the post data, then checks if the response was yes. If
        so, deletes the page. Otherwise redirects to the previous page.
        """
        post_id = self.request.get("post_id")
        resp_yes = self.request.get("yes")
        previous_page = self.get_previous_page()

        if resp_yes:
            # If the user came from the post page, redirect to the front page
            if previous_page == post_id:
                previous_page = "/"
            post = BlogPosts.by_id(int(post_id))
            post.delete()

        # delete previous cookies
        self.set_secure_cookie('page', "")
        self.set_secure_cookie('post_id', "")
        self.redirect('/%s' % previous_page) 

class EditPageHandler(BlogHandler):
    """Handler for './editpost'."""
    def render_edit_page(self, subject = "", content = "", error = ""):
        """
        Renders the edit post page.

        Gets the post id, then checks that,
        if subject or content hasn't been sent. If either hasn't, then
        checks if the post id has data. If it does, checks that is was
        created by the current user. If it wasn't, then the user is
        redirected to the previous page. If it was, then sets the
        subject and content for the post and renders the page.
        Otherwise, redirects the user to previous page.
        """
        post_id = self.read_secure_cookie("post_id")
        previous_page = self.get_previous_page()

        if not subject or not content:
            if post_id:
                post = BlogPosts.by_id(int(post_id))
                if not str(self.user.key().id()) == str(post.created_user_id):
                    self.redirect('/%s' % previous_page)
                else:
                    subject = post.subject
                    content = post.content
            else:
                self.redirect('/%s' % previous_page)
        self.render('edit_post.html',
                    user = self.user,
                    subject = subject,
                    content = content,
                    error = error)

    def get(self):
        """
        For the first time rendering the page. Checks if the user is
        logged in. If so, renders the edit page. Otherwise, redirects
        to the signup page.
        """
        if self.user:
            self.render_edit_page()
        else:
            self.redirect('/signup')

    def post(self):
        """
        Gets the posted/ cookie data, then checks if both subject
        and content have data. If they do, edits the post, deletes
        the cookies, and redirects to the previous page. Otherwise,
        renders the page with the error.
        """
        subject = self.request.get("subject")
        content = self.request.get("content")
        post_id = self.read_secure_cookie("post_id")
        previous_page = self.get_previous_page()

        if subject and content:
            if post_id:
                post = BlogPosts.by_id(int(post_id))
                post.subject = subject
                post.content = content
                post.put()

                self.set_secure_cookie("post_id", "")
                self.set_secure_cookie("page", "")
                self.redirect('/%s' % previous_page)
            else:
                error = "We need both a subject and some content!"
                self.render_edit_page(subject, content, error)
        else:
            error = "We need both a subject and some content!"
            self.render_edit_page(subject, content, error)

class NewCommentPageHandler(BlogHandler):
    """Handler for the './newcomment' page."""

    def render_new_comment_page(self, subject="", content="", error=""):
        """Renders the new comment page with the given content."""
        post_id = self.read_secure_cookie("post_id")
        if post_id:
            post = BlogPosts.by_id(int(post_id))
            if not post:
                self.redirect('/')
            else:
                self.render("new_comment.html", 
                            subject=subject,
                            content=content,
                            error=error,
                            user=self.user)
        else:
            self.redirect('/')

    def get(self):
        """
        Checks if the user is signed it. If so, renders the new
        comment page. Otherwise, redirects to the sign up page.
        """
        if self.user:
            self.render_new_comment_page()
        else:
            self.redirect('/signup')

    def post(self):
        """
        Gets the submitted data, then checks if both subject and
        content exist. If so, adds the comment, deletes the cookies,
        and redirects to the previous page. Otherwise, renders the
        page with errors.
        """
        subject = self.request.get("subject")
        content = self.request.get("content")
        post_id = self.read_secure_cookie("post_id")
        previous_page = self.get_previous_page()

        if subject and content:
            Comments.add_comment(subject=subject,
                                 content=content,
                                 post_id=post_id,
                                 user_id=str(self.user.key().id()))

            self.set_secure_cookie("post_id", "")
            self.set_secure_cookie("page", "")
            self.redirect("/%s" % previous_page)
        else:
            error = "We need both a subject and some content!"
            self.render_new_post(subject, content, error)

class EditCommentHandler(BlogHandler):
    """Handler for the './editcomment' page."""

    def render_edit_comment(self, subject = "", content = "", error = ""):
        """
        Renders the edit comments page.

        Checks if theres a subject
        or content. If neither then checks if there's a comment id.
        If there's not, redirects to the the previous page. If there
        is, gets the comment from the db, then checks that the user
        created the comment. If not, redirects to the previous page.
        If so, sets the comment subject and content, then renders
        the edit comment page.
        """
        comment_id = self.read_secure_cookie("comment_id")
        previous_page = self.get_previous_page()
        
        if not subject or not content:
            if comment_id:
                comment = Comments.by_id(int(comment_id))
                if comment:
                    if not str(self.user.key().id()) == str(
                        comment.created_user_id):
                        self.redirect('/%s' % previous_page)
                    else:
                        subject = comment.subject
                        content = comment.content
                else:
                    self.redirect('/%s' % previous_page)
            else:
                self.redirect('/%s' % previous_page)
        self.render('edit_comment.html',
                    user = self.user,
                    subject = subject,
                    content = content,
                    error = error)

    def get(self):
        """
        Checks if the user is signed it. If so, renders the edit
        comment page. Otherwise, redirects to the sign up page.
        """
        if self.user:
            self.render_edit_comment()
        else:
            self.redirect('/signup')

    def post(self):
        """
        Gets the submitted data, then checks if subject and content
        have data. If so, edits the comment, delets the comment_id
        and page cookies, then redirects to the previous page.
        Otherwise, renders the page with the error.
        """
        subject = self.request.get("subject")
        content = self.request.get("content")
        comment_id = self.read_secure_cookie("comment_id")
        previous_page = self.get_previous_page()

        error = "We need both a subject and some content!"

        if subject and content:
            if comment_id:
                comment = Comments.by_id(int(comment_id))
                comment.subject = subject
                comment.content = content
                comment.put()

                self.set_secure_cookie("comment_id", "")
                self.set_secure_cookie("page", "")
                self.redirect('/%s' % previous_page)
            else:
                self.render_edit_comment(subject, content, error)
        else:
            self.render_edit_comment(subject, content, error)

class DeleteCommentPageHandler(BlogHandler):
    """Handler for './deletecomment' page."""

    def render_delete_comment_page(self):
        """
        Renders the delete comment page.

        Gets the cookies, checks that the comment with the comment id
        exists and was created by the user. If it was, renders the
        delete comment page. Otherwise, redirects the user to the
        previous page.
        """
        comment_id = self.read_secure_cookie("comment_id")
        previous_page = self.get_previous_page()

        if comment_id:
            comment = Comments.by_id(int(comment_id))
            if comment:
                if not (
                    str(self.user.key().id()) == str(comment.created_user_id)):
                    self.redirect('/%s' % previous_page)
                self.render('delete_comment.html',
                             user=self.user,
                             comment_id=comment_id)
            else:
                self.redirect('/%s' % previous_page)    
        else:
            self.redirect('/%s' % previous_page)

    def get(self):
        """
        Checks if the user is logged in. If so, renders the delete
        comment page. Otherwise, renders the delete comment page.
        """
        if self.user:
            self.render_delete_comment_page()
        else:
            self.redirect('/signup')

    def post(self):
        comment_id = self.request.get("comment_id")
        resp_yes = self.request.get("yes")
        resp_no = self.request.get("no")
        previous_page = self.get_previous_page()

        if resp_yes:
            comment = Comments.by_id(int(comment_id))
            comment.delete()

        # delete previous cookies
        self.set_secure_cookie('page', "")
        self.set_secure_cookie('post_id', "")
        self.redirect('/%s' % previous_page)        

class LoginPageHandler(BlogHandler):
    """Handler for the login page, './login'."""
    def render_login_page(self, password_error=""):
        """Renders the login page."""
        self.render('login.html')

    def get(self):
        """
        Checks if the user is logged in. If so, redirects to the
        welcome page. Otherwise, renders the login page.
        """
        if self.user:
            self.redirect('/welcome')
        else:
            self.render_login_page()

    def post(self):
        """
        Gets the posted data, then attempts to log the user in. If
        it's successful, redirect the user to the welcome page.
        Otherwise, render the login page with the error.
        """
        self.username = self.request.get('username')
        self.password = self.request.get('password')
        logging.info("attempting login")
        logging.info('username is %s' % self.username)
        logging.info('password is %s' % self.password)

        user = Users.user_login(self.username, self.password)
        
        if user:
            self.page_login(user)
            self.redirect('/welcome')
        else:
            logging.info("login failed")
            self.render_login_page(password_error = "Invalid Login")

class LogoutHandler(BlogHandler):
    """
    Handler for './logout'. Deletes the user cookie and redirects to
    the front page.
    """
    def get(self):
        self.logout()
        self.redirect('/blog')

class HomePageHandler(BlogHandler):
    """
    Handler for the base url. Redirects the user to the blog
    front page.
    """
    def get(self):
        self.redirect('/blog')       

      
app = webapp2.WSGIApplication([('/blog', BlogFrontPageHandler),
                               ('/newpost', NewPostPageHandler),
                               ('/(\d+)', StaticPostPageHandler),
                               ('/signup', UserSignUpHandler),
                               ('/welcome', WelcomePageHandler),
                               ('/login', LoginPageHandler),
                               ('/logout', LogoutHandler),
                               ('/', HomePageHandler),
                               ('/myposts', MyPostsPageHandler),
                               ('/deletepost', DeletePostPage),
                               ('/editpost', EditPageHandler),
                               ('/newcomment', NewCommentPageHandler),
                               ('/editcomment', EditCommentHandler),
                               ('/deletecomment', DeleteCommentPageHandler)
                               ],
                               debug=True)