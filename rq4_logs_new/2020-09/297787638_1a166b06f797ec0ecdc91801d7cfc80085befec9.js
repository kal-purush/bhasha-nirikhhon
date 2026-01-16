const mongoose = require('mongoose');
const Schema = mongoose.Schema;

let Users = new Schema({
    firstName: {
        type: String
    },
    middleName: {
        type: String
    },
    lastName: {
        type: String
    },
    nationality: {
        type: String
    },
    sport: {
        type: String
    },
    gender: {
        type: String
    },
    state: {
        type: String
    },
    birsthday: {
        type: Date
    },
    location: {
        type: String
    },
    street1: {
        type: String
    },
    street2: {
        type: String
    },
    county: {
        type: String
    },
    zip: {
        type: Number
    },
    interest: {
        type: String
    },
    phone: {
        type: String
    },
    association: {
        type: String
    },
    married: {
        type: String
    },
    charities: {
        type: String
    },
    team: {
        type: String
    },
    sociallink: {
        type: String
    },
    description: {
        type: String
    },
}, {
    timestamps: true
},{
    colletion: 'Users'
});

module.exports = mongoose.model('Users', Users);