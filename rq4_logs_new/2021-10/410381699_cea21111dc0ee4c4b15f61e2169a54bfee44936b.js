const db = require("../models");
const bcrypt = require('bcrypt');
const User = db.users;

exports.createUser = (req, res) => {
    // Validate request
    if (!req.body.username) {
        res.status(400).send({ message: "Content can not be empty!" });
        return;
    }

    let body = req.body;
    console.log(body)
    console.log(typeof +process.env.DB_SALT)

    // Hash password 
    bcrypt.hash(body.password, +process.env.DB_SALT, (err, hash) => {
        let newUser = {
            avatar: body.avatar,
            username: body.username,
            email: body.email,
            password: hash
        }
        
        let tempUser = new User(newUser);

        // Saves new user to the database
        tempUser
        .save(tempUser)
        .then(data => {
            res.send(data);
        })
        .catch(err => {
            res.status(500).send({
                message:
                err.message || "Some error occurred while creating the Tweet."
            });
        });
    })
}

exports.loginUser = (req, res) => {
    User.findOne({username: req.body.username}, function (err, user) {
        console.log('This is my login user ', user);
        bcrypt.compare(req.body.password, user.password, (err, response) => {
            console.log(response)
            if(response) {
                res.status(200).send({ message: "Login credentials validated" });
            }
        })
    });
}