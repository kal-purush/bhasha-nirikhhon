const express = require('express');
const bcrypt = require('bcryptjs');

const jwt = require('jsonwebtoken');
const tokenProvider = require('../config/token.json');

const router = express.Router();

const User = require('../models/user');

router.post('/register', async (req, res) => {
    const { email, telephone } = req.body;
    try {
        if(await User.findOne({email}))
            return res.status(400).send({error: 'email already exist'});
        if(await User.findOne({telephone}))
            return res.status(400).send({error: 'telephone already exist'});

        let user = await User.create(req.body);
        const token = jwt.sign({ id: user._id }, tokenProvider.secret, {
            expiresIn: 311040000
        });
        return res.send({token});
    } catch (error) {
        return res.status(400).send(error);   
    };
});

router.post('/', async (req, res) => {
    const { email, password } = req.body;
    try {
        const user = await User.findOne({email}).select('+password');

        if(!user)
            return res.status(401).send({error: 'user doesnt exist'});
        if(!await bcrypt.compare(password, user.password))
            return res.status(401).send({error: 'invalid password'});
        
        user.password = undefined;
        const token = jwt.sign({ id: user._id }, tokenProvider.secret, {
            expiresIn: 311040000
        });
        return res.send({token});
    } catch (error) {
        return res.status(500).send(error);
    };
});

//TWO FACTOR AUTHENTICATION
// FIRST TIME: REGISTER WITH TELEPHONE AND COMPLETE OTHERS INPUTS ON LAZY REGISTRATION
// NEXT ALL TIMES: LOGIN WITH AUTH CONFIRM WITH TELEPHONE NUMBER TO PICK USER INFOS

module.exports = app => app.use('/auth', router);