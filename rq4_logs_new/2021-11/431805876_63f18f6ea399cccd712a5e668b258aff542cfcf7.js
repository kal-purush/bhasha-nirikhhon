'use strict';
const mongoose = require('mongoose');
const bcrypt = require('bcrypt');
const Schema = require("mongoose/lib/schema");
const jwt = require('jsonwebtoken');
const userPaginate = require('mongoose-aggregate-paginate-v2');

const authorSchema = new Schema(
    {
        title: {
            type: String,
            text: true,
            maxlength: 50
        },
        content: {
            type: String,
            text: true,
            maxlength: 50
        },
        organizer: {
            type: mongoose.Schema.Types.ObjectId,
            ref: 'Organizer'
        },

    },
    {
        timestamps: { createdAt: 'created_at', updatedAt: 'last_updated' }
    }
);

authorSchema.statics = {
    async signUp(payload) {
        let {email, password} = payload;
        if(!email || !password){
            return {
                success: false,
                message: `Enter a ${!email ? 'email' : 'password'}`,
                data: {}
            };
        }
        //TODO create wallet
        let user = await this();
        user.firstname = payload.firstname;
        user.lastname = payload.lastname;
        user.email = payload.email;
        user.password = bcrypt.hashSync(payload.password, 10);
        user.email = email;
        await user.save();
        return {
            success: true,
            message: 'User successfully saved',
            data: {}
        };
    },
    async login(payload) {
        let {email, password} = payload;
        let foundUser = await this.findOne({'email': email}).exec();
        const match = await bcrypt.compare(password, foundUser.password);
        if(match){
            return {
                success: true,
                message: 'Login successful',
                token: jwt.sign({ _id: foundUser._id }, process.env.JWT_PRIVATE_KEY),
                data: {
                    user : foundUser,
                    tags: await tagModel.find({}),
                    categories: await categoryModel.find({})
                }

            };
        }else{
            return {
                success: false,
                message: 'username/password not found',
                data: {}
            };
        }
    },
}

authorSchema.plugin(userPaginate);
module.exports = mongoose.model('Author', authorSchema);