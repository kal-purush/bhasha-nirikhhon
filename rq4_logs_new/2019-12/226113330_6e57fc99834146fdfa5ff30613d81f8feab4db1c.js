const mongoose = require('mongoose');
const User = require('../models/user');
const openDbConnection = require('../middleware/db');
openDbConnection();

const friendSchema = new mongoose.Schema({
    userId: {
        type: mongoose.Schema.Types.ObjectId,
        ref: 'User',
        required: true,
    },
    friendId: {
        type: mongoose.Schema.Types.ObjectId,
        ref: 'User',
        required: true,
    }/*,
    circle:{
        type: String,
        required: true,
        minlength: 2,
        maxlength: 10
   }*/
});

const Friend = mongoose.model('Friend', friendSchema);

async function insertFriend(Data) {
    let friend = new Friend({
        userId: Data.userId,
        friendId: Data.friendId
    });

    try {
        const result = await friend.save();
        console.log(result);
        
        const user = await User.User.findOne({"_id":result.userId});
        const friends = await Friend.find({"userId":result.userId}).select({"friendId": 1});

        user.listOfFriends = friends;
        user.save();

        return result;
    } catch (ex) {
        console.log(ex.message);
        return undefined;
    }
}

async function createFriend(Data) {
    if(Data.userId == Data.friendId){
        return {
            message: 'It is you',
            status: 'failed'
        }
    } else{
        const newFriendship = await insertFriend(Data);
        if (newFriendship) {
            return {
                status: 'ok',
                friendshipId: newFriendship._id
            }
        }
}}

exports.createFriend = createFriend; 
exports.Friend = Friend;