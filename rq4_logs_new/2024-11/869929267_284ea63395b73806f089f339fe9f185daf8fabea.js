const userModel = require('../models/user')
const bcrypt = require('bcrypt')
const {generateToken ,sendResetPasswordEmail , varifyToken} = require('../utils/index')
const resetPassword = () => {
    const requestPasswordReset = async (req, res) => {
        const email = req.body.email;
        try{    
            const user = await userModel.findOne({email:email});
            if (!user) return res.status(400).json({ message: "User doesn't exist" });            
            const token = generateToken({ id: user._id, email: user.email });
            const resetURL = `http://localhost:5174/resetpassword/${user._id}/${token}`;
            await sendResetPasswordEmail(user.email, resetURL); 
            res.status(200).json({ message: 'Password reset link sent' });
        }
        catch (error){
            res.status(400).json({ message: error.message });
        }
    }

    const resetPassword = async (req, res) => {
        const { id, token } = req.params;
        const { password } = req.body;
        try{
            const user = await userModel.findOne({ _id: id });
            if (!user) return res.status(400).json({ message: "User doesn't exist" });
            const isValidToken = varifyToken(token);
            if (!isValidToken) return res.status(400).json({ message: "Invalid token" });
            const salt = await bcrypt.genSalt(10);
            const hashedPassword = await bcrypt.hash(password, 10);
            await userModel.updateOne(
                {
                  _id: id,
                },
                {
                  $set: {
                    password: hashedPassword,
                  },
                }
              );
            await user.save();
            res.status(200).json({ message: "Password reset successfully" });
        }
        catch (error){
            res.status(400).json({ message: error.message });
        }
    }

    return {
        requestPasswordReset,
        resetPassword
    }
} 

module.exports = resetPassword