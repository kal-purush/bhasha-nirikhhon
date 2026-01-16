const { User } = require("../models");

const freindController = {
  addFriend({ params, body }, res) {
    User.findOneAndUpdate(
      { _id: params.userId },
      { $addToSet: { friends: params.friendId } },
      { new: true }
    )
      .then((dbFriendData) => {
        if (!dbFriendData) {
          res.status(404).json({ message: "No user found with this id!" });
          return;
        }
        res.json(dbFriendData);
      })
      .catch((err) => res.status(400).json(err));
  },

  deleteFriend({ params, body }, res) {
    User.findOneAndUpdate(
      { _id: params.userId },
      { $pull: { friends: params.friendId } },
      { new: true }
    )
      .then((dbFriendData) => {
        if (!dbFriendData) {
          res.status(404).json({ message: "No user found with this id!" });
          return;
        }
        res.json(dbFriendData);
      })
      .catch((err) => res.status(400).json(err));
  },
};

module.exports = freindController;