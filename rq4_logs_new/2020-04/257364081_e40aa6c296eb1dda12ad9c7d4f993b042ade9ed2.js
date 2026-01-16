const User = require('../models/users');
const _ = require('lodash');
const bcrypt = require('bcrypt');

const loginUser = async (req, res) => {
    let user = await User.findOne({ email:req.body.email });
    if(!user) res.status(400).send("Invalid Email or password");
    else {
        const validPassword = await bcrypt.compare(req.body.password, user.password);
        if(!validPassword) res.status(400).send("Invalid Email or password");
        else {
            const token = user.generateAuthToken();
            res.send({
                token: token,
                name: user.name
            });
        }
    }
}

const registerUser = async (req, res) => {
    let user = await User.findOne({ email:req.body.email });
    if(user) res.status(400).send({ 'message':0 });
    else {
        const salt = await bcrypt.genSalt(10);  
        let password = await bcrypt.hash(req.body.password, salt);
        let obj = {
            name: req.body.name,
            email: req.body.email,
            password: password,
            phone_no: req.body.phone_no,
            city: req.body.city,
            is_merchant: req.body.is_merchant
        }
        User.create(obj)
            .then((result) =>{
                res.send({ 'message':1 });
            })
            .catch((err) => {
                res.status(500).send({ 'message':0 });
            });
    }
}

module.exports = {
    loginUser,
    registerUser
}