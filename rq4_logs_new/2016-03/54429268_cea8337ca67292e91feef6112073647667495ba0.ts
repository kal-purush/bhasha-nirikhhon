/// <reference path="vote.ts" />




module Journal {
  export interface IEntry {
    title: string;
    body: string;
    votes: number;
    id: number;
  }

  export class Entry {
    votes: number = 0;
    constructor(public title: string, public body: string, public id: number){}
    upVote() {
      this.votes += 1;
    }
    downVote() {
      this.votes -= 1;
    }
  }
}

$(document).ready(function (){

var posts = [];

  $("#publish").submit(function(event) {
    event.preventDefault();
    var title = $("#title").val();
    var entry = $("#entry").val();

    $("#title").val('');
    $("#entry").val('');

    var currentEntry = new Journal.Entry(title, entry, posts.length);

    posts.push(currentEntry);

    $("#postArea").html('');

    for(var i = 0; i < posts.length; i++) {
      $("#postArea").append("<h4>" + posts[i].title + "</h4>" +
                            "<p>" + posts[i].body + "</p>" +
                            "<button id='" + posts[i].id + "' class='upVote'>UPVOTE</button>" +
                            "<button id='" + posts[i].id + "' class='downVote'>DOWNVOTE</button>");
      $("#postArea").append(posts[i].votes);
    }
  });

  $(document.body).on('click', '.upVote', function() {
    var selectedEntry = Journal.findById(this.id, posts);
    selectedEntry.upVote();
    $("#postArea").html('');

    posts.sort(function (a,b) {
      return b.votes-a.votes
    });
    for(var i = 0; i < posts.length; i++) {
      $("#postArea").append("<h4>" + posts[i].title + "</h4>" +
                            "<p>" + posts[i].body + "</p>" +
                            "<button id='" + posts[i].id + "' class='upVote'>UPVOTE</button>" +
                            "<button id='" + posts[i].id + "' class='downVote'>DOWNVOTE</button>");
      $("#postArea").append(posts[i].votes);
    }
  });

  $(document.body).on('click', '.downVote', function() {
    var selectedEntry = Journal.findById(this.id, posts);
    selectedEntry.downVote();
    $("#postArea").html('');

    posts.sort(function (a,b) {
      return b.votes-a.votes
    });
    for(var i = 0; i < posts.length; i++) {
      $("#postArea").append("<h4>" + posts[i].title + "</h4>" +
                            "<p>" + posts[i].body + "</p>" +
                            "<button id='" + posts[i].id + "' class='upVote'>UPVOTE</button>" +
                            "<button id='" + posts[i].id + "' class='downVote'>DOWNVOTE</button>");
      $("#postArea").append(posts[i].votes);
    }
  });



});