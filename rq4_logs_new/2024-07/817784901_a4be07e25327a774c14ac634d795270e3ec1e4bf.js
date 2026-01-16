const User = require("../models/user");

const getUnderAdmin= async(req,res)=>{
    const {email}= req.body;
    const adminToSee= await User.findOne({email});
    if (!adminToSee) return res.status(404).json({message: "Admin not found"});
    const invitedBy= adminToSee.id;
    const usersUnderAdmin= await User.find({invitedBy});
    if (!usersUnderAdmin) return res.status(400).json({message: "No users under this admin."})
    return res.status(200).json(usersUnderAdmin)
}

module.exports= getUnderAdmin