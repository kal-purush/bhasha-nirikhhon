const mongoose = require('mongoose')

const sheetSchema = new mongoose.Schema({
    name: String,
    category: String
})

module.exports = mongoose.model('Sheet', sheetSchema)