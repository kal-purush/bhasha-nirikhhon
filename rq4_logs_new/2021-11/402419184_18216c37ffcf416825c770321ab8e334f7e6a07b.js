const { error500, error400 } = require("../util/res");
const { ObjectId } = require("mongodb");
const User = require("../models/User");
const FriendRequest = async (req, res) => {
  try {
    const { userId, type } = req.body;
    let user = await User.findById(userId);
    console.log(findUser)
    if (type == 1) {
      const findUser = user.friendsRequest.find(
        (e) => e.toString() == req.userId
      );
      if (!findUser) user.friendsRequest.push(ObjectId(req.userId));
    } else
      user.friendsRequest = user.friendsRequest.filter(
        (e) => e.toString() != req.userId
      );
    await user.save();
    return res.json({ success: true, message: "save success" });
  } catch (error) {
    console.log(error);
    return error500(res);
  }
};
const FriendRequestRespone = async (req, res) => {
  try {
    const { userId, type } = req.body;
    let user = await User.findById(req.userId);
    if (type == 1) {
      const findUser = user.friends.find((e) => e.toString() == userId);
      if (!findUser) user.friends.push(ObjectId(userId));
    }
    user.friendsRequest = user.friendsRequest.filter(
      (e) => e.toString() != userId
    );
    await user.save();
    return res.json({ success: true, message: "save success" });
  } catch (error) {
    return error500(res);
  }
};

module.exports = {
  FriendRequest,
  FriendRequestRespone,
};