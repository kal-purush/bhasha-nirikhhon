const Comment = require('../models/comment.model');

const addComment = async (req, res) => {
  const { content, postID } = req.body;
  const { id } = req.user;

  const comment = new Comment({
    content,
    userIDcomment: id,
    postIDcomment: postID,
  });
  console.log(comment);
  return res.json({
    comment,
  });
};

module.exports = { addComment };