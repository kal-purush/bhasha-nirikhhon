const mongoose = require('mongoose')

const UserSchema = new mongoose.Schema({
    name: {
        type: String,
        required: true
    },
    email: {
        type: String,
        required: true,
        unique: true
    },
    password: {
        type: String,
        required: true,
    },
   
    register_date: {
        type: Number,
        default: Date.now
    },
   
})

module.exports = mongoose.model('user', UserSchema)