'use strict';

var models = require('../../models/weather');

//Function for Creating Account Collection
exports.accountCollectionCreation = function(source, destination, amount) {

    var account = new models.Account();
    account.source = source;
    account.destination = destination;
    account.amount = amount;
    account.save(function(err, body) {
        if (err || !body) {
            console.log(err);
        }

    });

}