const mongoose = require('mongoose')

const userSchema = mongoose.Schema(
    {
        name : {
            type : String
        },
        email : {
            type : String,
            required : true
        },
        password : {
            type : String,
            required : true
        }
    },
    {
        timestamps : true
    }
)

module.exports = mongoose.model('user', userSchema)