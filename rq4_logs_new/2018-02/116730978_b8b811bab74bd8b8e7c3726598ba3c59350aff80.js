"use strict";
function post() {

    $.get({url: "data/blog.json"}).done(function (data) {
        data.forEach(function (post, index, array) {
            var posts = "";

            posts = "<div>" + "<h2>" + post.title + "</h2>" + "<p>" + post.content + "</p>" + "<p>" + post.date + "</p>" + "</div>";

            $("#posts").append(posts);
        })
    });
}

post();