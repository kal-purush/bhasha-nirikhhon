const mongoose = require('mongoose');
const { Schema } = mongoose;

// Cart Schema
const cartSchema = new Schema({
    ItemId: String,
    name: {
        type: String,
        trim: true,
        required: true,
        minlength: 3
    }
    }
});
