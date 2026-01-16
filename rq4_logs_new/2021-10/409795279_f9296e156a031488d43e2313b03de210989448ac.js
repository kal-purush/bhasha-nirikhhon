const router = require('express').Router();
const { User, Post, Comment } = require('../../models');

router.post('/', async (req, res) => {
    try {
      const userComment = await Comment.create({
          body: req.body.body,
          user_id : req.session.user_id
      });
        res.status(200).json(userComment);
    } catch (err) {
      res.status(400).json(err);
    }
  });


module.exports = router;