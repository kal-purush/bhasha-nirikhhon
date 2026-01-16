var Post = require("../models/post.model");

const post_controller = {
  showPosts: async (req, res) => {
    if (!req.user) {
      return res.redirect("/users/loginpage");
    }
    await Post.find((err, data) => {
      if (err) {
        // Internal server error
        res.status(500).send({ msg: "Internal Server Error" });
      } else {
        // OK
        if (data.length !== 0) {
          console.log("from post controller --> ", data);
          // ok
          res.render("posts", { username: req.user, posts: data });
        } else {
          // no data
          res.render("posts", { username: req.user, posts: data });
        }
      }
    });
  },

  createPost: (req, res) => {
    let entry = new Post({
      Post_Title: req.body.Post_Title,
      Description: req.body.Description,
      Author: req.body.Author,
    });

    entry.save(function (err) {
      if (err) {
        // not acceptable
        res.status(406).send(err.message);
      } else {
        // created
        res.redirect("/posts/postspage");
      }
    });
  },

  deletePost: (req, res) => {
    Post.deleteOne({ _id: req.body.id }, function (err) {
      // bad request
      if (err) res.status(400).send("Invalid id");
      // ok, successfully deleted
      else res.redirect("/posts/postspage");
    });
  },

  addComment: (req, res) => {
    console.log("from add comments --> ", req.body.id, req.body.comment_body);
    Post.findByIdAndUpdate(
      { _id: req.body.id },
      {
        $push: {
          Comments: req.body.comment_body,
        },
      }
    )
      .then(res.redirect("/posts/postspage"))
      .catch((err) => console.log(err));
  },
};

module.exports = post_controller;