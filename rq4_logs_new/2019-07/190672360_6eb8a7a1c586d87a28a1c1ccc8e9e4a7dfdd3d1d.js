const mongoose = require('mongoose');
var bcrypt = require('bcryptjs');

const { Schema } = mongoose;

const adminSchema = new Schema({
    username: String,
    password: String
});

// pre here is a built in middleware that will run before the 
// function that we past in as first arg
// in this example its the save function
// note: a normal function signature is used here instead of an arrow function
// to allow for the tind binding in the fuction below
adminSchema.pre('save', async function (next) {
    const admin = this;

    if (admin.isModified('password')) {
        admin.password = await bcrypt.hash(admin.password, 8);
    }

    // next here will move in the saving process
    next();
});

// static methods are availble on the Model 
adminSchema.statics.findByCredentials = async (username, password) => {
    // usage of destructuing here
    const admin = await Admin.findOne({ username });

    if (!admin) {
        throw new Error("Unable to find that username");
    };

    const isMatch = await bcrypt.compare(password, admin.password);

    if (!isMatch) {
        throw new Error("Unable to login");
    };

    return admin;
};

// setting up our model creation and export like this allows
// us to reuse the Admin models for querying the db from wothin this schema creation itself
const Admin = new mongoose.model('admin', adminSchema);

module.exports = Admin;