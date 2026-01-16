const mongoose = require('mongoose');

const HistoricalDataSchema = new mongoose.Schema({
    symbol: {type: String, required: true, unique: true},
    dateData: [{
        date: Number,
        price: Number,
    }]
})

const HistoricalData = mongoose.model("historicalData", HistoricalDataSchema)

module.exports = HistoricalData;