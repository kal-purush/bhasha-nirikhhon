const mongoose = require('mongoose');
const { ObjectId } = mongoose.Schema;

const RentalModel = mongoose.Schema({
    location: {
        type: String,
        required: true,
    },
    type: {
        type: String,
        required: true,
    },
    bedrooms: {
        type: String,
        required: true,
    },
    baths: {
        type: String,
        required: true,
    },
    wePay: {
        type: String
    },
    phone:  {
        type: Number,
        required: true, 
    },
    price: {
        type: Number,
        required: true, 
    },
    comments: {
        type: String
    },
    imageURL: {
        type: String
    }, 
    createOn: {
        type: String
    }
});

module.exports = mongoose.model('rentals', RentalModel);