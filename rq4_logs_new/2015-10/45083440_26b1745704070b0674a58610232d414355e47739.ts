/// <reference path='../../libs/underscore/underscore.d.ts' />

/// <reference path='BlogPost.ts' />

module blogposts {
  'use strict';

  export class InMemoryBlogPostStore {
    private posts = [
        new BlogPost(1, "First Post", "This is the body"),
        new BlogPost(2, "Second Post", "This is the body"),
        new BlogPost(3, "Third Post", "This is the body"),
        new BlogPost(4, "Fourth Post", "This is the body")
    ];

    add(newPost: BlogPost) {
      this.posts.push(newPost);
    }

    edit(editedPost: BlogPost) {
      var index = _.findIndex(this.posts,
        function(post) {return post.id == editedPost.id});
      if (index != -1) {
          this.posts[index] = editedPost;
      } else {
          throw "No post with id " + editedPost.id;
      }
    }

    get(id: number) {
      var index = _.findIndex(this.posts,
          function(post) {
            return post.id == id;
          });
      console.log("Found " + index);
      return this.posts[index];
    }

    remove(id: number) {
      var newPosts = _.filter(this.posts,
        function(post) { return post.id != id});
      var difference = this.posts.length - newPosts.length;
      this.posts = newPosts;
      return difference;
    }

    list() {
      return this.posts;
    }

    nextId() {
      return this.posts.length + 1;
    }
  }
}