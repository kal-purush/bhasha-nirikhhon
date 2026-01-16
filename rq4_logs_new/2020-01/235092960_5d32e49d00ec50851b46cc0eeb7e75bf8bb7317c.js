const { USER } = require('../schema/index');
const mongoose = require('mongoose');
require("dotenv").config();
const HttpStatus = require('http-status-codes');
const jwt = require('jsonwebtoken');
const bcrypt = require('bcryptjs');

module.exports.createUser = (req, res) => {
  const newUser = new USER({
    _id: new mongoose.Types.ObjectId,
    username: req.body.username,
    password: req.body.password,
    firstname: req.body.firstname,
    lastname: req.body.lastname,
    address: req.body.address,
    city: req.body.city,
    email: req.body.email
  });
  bcrypt.genSalt(10, (err, salt) => {
    bcrypt.hash(newUser.password, salt, (err, hash) => {
      if (err) throw err;
      newUser.password = hash;
      newUser
        .save()
        .then(user => res.status(HttpStatus.OK).json({ user }))
        .catch(err => res.status(HttpStatus.BAD_REQUEST).json(err));
    });
  });
}

module.exports.listUser = (req, res) => {
  USER.find()
  .then(users => {
    res.status(200).json({user: users});
  })
  .catch(err => {
    res.status(HttpStatus.BAD_REQUEST).json({err: 'List Not Found'});
  });
}

module.exports.readUser = (req, res) => {
  res.status(200).json({user: req.user});
}

module.exports.validateUser = (req, res) => {
  const username = req.body.username;
  const password = req.body.password;
  USER.find()
  .then(users => {
    authenticatedUser = users.find(user => user.username === username && authenticatedUser.password === password);
    if (!authenticatedUser) {
      res.status(404).json({error: "username or password is wrong"})
    }
    else if(authenticatedUser) {
      res.status(200).json({message: "you have successfully logged in"});
    }
  })
  .catch(err => {
    res.status(500).json({error: err});
  })
}

module.exports.generateToken = (req, res) => {
  const token = jwt.sign(
    { id: "5e295475f70b71184b85b7b0" },
    process.env.JWT_SECRET
  );
  res.status(HttpStatus.OK).json({ token });
}

module.exports.loginUser = (req, res) => {
  const token = jwt.sign({id: req.user._id}, process.env.JWT_SECRET);
  res.status(200).json({token: token});
}