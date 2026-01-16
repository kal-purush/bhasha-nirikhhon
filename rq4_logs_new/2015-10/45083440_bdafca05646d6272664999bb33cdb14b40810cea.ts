/// <reference path='../../libs/underscore/underscore.d.ts' />

/// <reference path='BlogPost.ts' />
/// <reference path='IBlogPostStore.ts' />

module blogposts {
  'use strict';

  export class BlogPostStore implements IBlogPostStore {
    private posts = [
        new BlogPost(1, "First Post", "This is the body"),
        new BlogPost(2, "Second Post", "This is the body"),
        new BlogPost(3, "Third Post", "This is the body"),
        new BlogPost(4, "Fourth Post", "This is the body")
    ];

    add(newPost: BlogPost) {
      // posts.push(newPost);
    }

    remove(at: number) {
      //Todo
      throw "Not implemented yet.";
    }

    list() {
      return this.posts;
    }
  }
}