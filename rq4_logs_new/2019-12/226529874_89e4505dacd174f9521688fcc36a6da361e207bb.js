const mongoose = require('mongoose');

const survivorSchema = new mongoose.Schema({

    name: String, 
    age: Number,   
    sex: String, 
    latitude: Number, 
    longitude: Number,
    infected: Boolean,
    items: [String],
});

module.exports = mongoose.model("SurvivorModel", survivorSchema);
