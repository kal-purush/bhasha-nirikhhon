const mongoose = require('mongoose');
const bcrypt   = require('bcryptjs');
const config   = require('../config/database');

// Create user schema
const UserSchema = mongoose.Schema({
    firstName: {
        type:     String,
        required: true
    },
    lastName: {
        type:     String,
        required: true
    },
    email: {
        type:     String,
        required: true,
        unique:   true
    },
    username: {
        type:     String,
        required: true,
        unique:   true
    },
    password: {
        type:     String,
        required: true
    }
});

const USer = module.exports = mongoose.model('User', UserSchema);

// Find user by database Id
module.exports.getUserById = (id, callback) => {
    User.findById(id, callback);
};

// Find user by username
module.exports.getUserByUsername = (username, callback) => {
    // Username has to be set to a query so we set it to the username that was passed in the function
    const query = {username: username};
    // Now that we set the query to the username variable and pass the callback passed in the function
    User.findOne(query, callback);
};
// Adding a new user
module.exports.addUser = (newUser, callback) => {
    bcrypt.genSalt(10, (err, salt) =>{
        // Hash the newUser password that was passed in the addUser() function using bcrypt salt gen
        bcrypt.hash(newUser.password, salt, (err, hash) => {
            // Checking for error on creating password
            if (err) throw err;
            // If no error hash the new user password and set it to the newUser password before saving it to the database
            newUser.password = hash;
            // Save the new user
            newUser.save(callback)
        });
    });
};