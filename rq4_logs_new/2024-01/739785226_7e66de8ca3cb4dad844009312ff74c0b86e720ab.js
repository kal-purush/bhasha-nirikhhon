const UserModel = require("../models/usersModel");

const friendsControllers = {
  addFriend: async (req, res) => {
    try {
      const newFollow = {
        id: req.body.id,
        name: req.body.name,
        userName: req.body.userName,
      };

      const newFollower = {
        id: req.userId,
        userName: req.userName,
        name: req.name,
      };

      const follow = await UserModel.findById(newFollower.id);
      const follower = await UserModel.findById(newFollow.id);

      const isFollowing = follow.followings.find((elm) => elm.id == newFollow.id)

    //verifica se já está seguindo
      if (isFollowing) {
        follow.followings = follow.followings.filter((elm) => elm.id != newFollow.id);
        await follow.save();
        follower.followers = follower.followers.filter((elm) => elm.id != newFollower.id);
        await follower.save();
        return res.status(200).json({ msg: "parou de seguir" });
      }
      
      //Caso não estiver seguindo
      follow.followings.push(newFollow);
      await follow.save();
      follower.followers.push(newFollower);
      await follower.save();

      res.status(201).json({ msg: "seguindo" });
    } catch (error) {
      res.status(500).json({
        msg: "Erro interno. STATUS DO ERRO: 500",
        error: error,
      });
    }
  },
  FindFriends: async (req, res) => {
    try {
      const name = req.params.name
      const response = await UserModel.findOne({userName: name}).select('followers followings')
  
      if (!response) {
        return res.status(404).json({ msg: "user não encontrado" });
      }
      res.status(200).json(response)
    }catch(error){
      res.status(500).json({
        msg: "Erro interno. STATUS DO ERRO: 500",
        error: error,
      });
    }
  }
};

module.exports = friendsControllers;