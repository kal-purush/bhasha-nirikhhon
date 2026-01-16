var mongoose = require('mongoose');
var Schema = mongoose.Schema;

var schema = new Schema({
    currencyFrom:{type: String, required: true},
    currencyTo:{type: String, required: true},
    amount:{type: Number, required: true },
    amount_usd:{type: Number, required: true },
    amount_btc:{type: Number, required: true },
    transaction:{type: Schema.Types.ObjectId, ref:'Transaction'}
},{
    timestamps: true
});

module.exports = mongoose.model('receipt',schema);

