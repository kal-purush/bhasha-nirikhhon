import webapp2
import jinja2
import logging
import os
import urllib
from google.appengine.ext import blobstore
from google.appengine.ext.webapp import blobstore_handlers
from google.appengine.api import images

from models import *

# Set up template directory with the Jinja environment
template_dir = os.path.join(os.path.dirname(__file__), 'templates')
env = jinja2.Environment(loader = jinja2.FileSystemLoader(template_dir), autoescape = True)

# used by self.render()
def render_template(template, **params):
  t = env.get_template(template)
  return t.render(params)

class BaseHandler(webapp2.RequestHandler):
  def write(self, *a, **kw):
    self.response.write(*a, **kw)

  def render(self, template, **kw):
    self.response.write(render_template(template, **kw))

class Front(BaseHandler):
  def get(self):
    all_image_info = ImageInfo.query().fetch()
    self.render('front.html', all_image_info = all_image_info)


# A basic upload method from the docs
class SimpleMethod(BaseHandler):
  def get(self):
    upload_url = blobstore.create_upload_url('/upload')
    self.render('simple-uploader.html', upload_url = upload_url)

# Upload Handler
class UploadHandler(blobstore_handlers.BlobstoreUploadHandler):
  def post(self):

    # self.get_uploads() returns a list of BlobInfo objects
    upload_files = self.get_uploads('image') # 'image' is the name of the input field in the form

    # [<google.appengine.ext.blobstore.blobstore.BlobInfo object at 0x105c63a50>]
    # a BlobInfo object
    # logging.info(upload_files[0])
    logging.info(upload_files)


    # select the first BlobInfo object
    blob_info = upload_files[0]

    # create a Photo entity with the blob key of uploaded file
    image_info = ImageInfo(blob_key = blob_info.key())
    image_info.put()

    # redirect control to the ServerHander
    self.redirect('/serve/%s' % blob_info.key())

# Download / Blob-serving Handler

class ServeHandler(blobstore_handlers.BlobstoreDownloadHandler):
  def render(self, template, **kw):
    self.response.write(render_template(template, **kw))

  # serves the image using given the blob_info.key()

  def get(self, resource):

    # AgVNdqxMr5dUXnxPzPWR-A==
    logging.info(resource)

    # unquote replaces %xx escapes by their single-character equivalent
    #   example: unquote('%7Ehome') yields '~home'
    resource = str(urllib.unquote(resource))

    # get the blob info from a given BlobKey object
    # (is resource a BlobKey Object?)
    blob_info = blobstore.BlobInfo.get(resource)

    url = images.get_serving_url(blob_info)

    self.render('raw.html', url = url)

    # takes BlobKey obj, string key, or BlobInfo obj as parameter
    # and returns the blob
    # self.send_blob(blob_info)


# New Post Form Page
class NewPost(BaseHandler):
  def get(self):
    all_posts = Post.query().fetch()
    self.render('new-post.html', all_posts = all_posts)

  def post(self):
    title = self.request.get('title')
    post = Post(title = title)
    post.put()
    # self.redirect('/new-post')
    self.redirect('/post/%s' % post.key.id())

class PostEdit(BaseHandler):
  def get(self, post_id):
    post = Post.get_by_id(int(post_id))
    self.render('post-edit.html', post = post)

class PostPage(BaseHandler):
  def get(self, post_id):

    # NEXTTTTTTTTTTTTTTTTTTTTTTT todo:
    # query for images where their post_id property == this post's id
    # extract the url via blobkey
    # and add it to this template

    post = Post.get_by_id(int(post_id))

    # queries for all images relevant to this post
    post_images = ImageInfo.query(ImageInfo.post_key == post.key).fetch()

    # run get_serving_url() on all blob_keys
    image_urls = list()
    for img in post_images:
      # append each url to image_urls list
      image_urls.append(images.get_serving_url(img.blob_key))

    self.render('post-page.html',
      post = post,
      image_urls = image_urls)

# Create an image given a post_id
# this, the image is in reference to a post
class NewImage(blobstore_handlers.BlobstoreUploadHandler):
  def render(self, template, **kw):
    self.response.write(render_template(template, **kw))

  def get(self, post_id):

    post = Post.get_by_id(int(post_id))

    # include the post_id in the upload_url
    # see regex route handler for image-upload/post_id
    upload_url = blobstore.create_upload_url('/image-upload/%s' % post_id)

    # this query to show all images uploaded (for debugging)
    all_image_info = ImageInfo.query().fetch()
    self.render('new-image.html',
      post = post,
      upload_url = upload_url,
      all_image_info = all_image_info)

# handles upload of images which are in reference of a post
class ImageUploadHandler(blobstore_handlers.BlobstoreUploadHandler):

  # the post_id is passed in the upload url
  def post(self, post_id):

    # get post by id
    post = Post.get_by_id(int(post_id))

    # get the BlobInfo object from the uploaded file
    uploaded_file = self.get_uploads('my-image')[0]

    blob_key = uploaded_file.key()
    logging.info(blob_key)

    if uploaded_file:
      image = ImageInfo(
        blob_key = blob_key,
        post_key = post.key)
      image.put()

app = webapp2.WSGIApplication([
    ('/', Front),
    ('/new-post', NewPost),
    ('/post/([0-9]+)/?', PostPage),
    ('/post/([0-9]+)/edit/?', PostEdit),
    ('/simple', SimpleMethod),
    # handles simple upload
    ('/upload', UploadHandler),

    # presents a form for image upload
    ('/new-image/([0-9]+)/?', NewImage),

    # handles upload of images in reference to posts
    ('/image-upload/([0-9]+)/?', ImageUploadHandler),
    ('/serve/([^/]+)?', ServeHandler)
], debug=True)