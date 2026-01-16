var request = require('superagent');
require('superagent-proxy')(request);

var config = require('../config.json');
var account = config.account;
var password = config.password;
var service = config.service;

function get(callback) {
    var endpoint = service + config.authentication + config.token + config.type;

    request
        .get(endpoint)
        .proxy('http://172.31.90.146:3128')
        .auth(account,password)
        .send()

        .end(function (err,res) {

            callback(err,res);
        });
}

function createItem(callback) {
    var itemBase = {Content: 'ItemBdd'};

    request
        .post('https://todo.ly/api/items.json')
        .proxy('http://172.31.90.146:3128')
        .auth(account,password)
        .send(itemBase)

        .end(function (err,res) {

            callback(err,res);
        });
}

function deleteItem(itemId,callback) {

    request
        .delete('https://todo.ly/api/items/'+itemId+'.json')
        .proxy('http://172.31.90.146:3128')
        .auth(account,password)

        .end(function (err,res) {

            callback(err,res);
        });
}

function updateItem(itemId,updateInfo,callback) {

    request
        .put('https://todo.ly/api/items/'+itemId+'.json')
        .proxy('http://172.31.90.146:3128')
        .auth(account,password)
        .send(updateInfo)

        .end(function (err,res) {

            callback(err,res);
        });
}

function getItemFromFilter(callback) {

    request
        .get('https://todo.ly/api/filters/0/items.json')
        .proxy('http://172.31.90.146:3128')
        .auth(account,password)
        .send()

        .end(function (err,res) {

            callback(err,res);
        });
}

function getListOfFilters(callback) {

    request
        .get('https://todo.ly/api/filters.json')
        .proxy('http://172.31.90.146:3128')
        .auth(account,password)
        .send()

        .end(function (err,res) {

            callback(err,res);
        });
}

function getDoneItems(callback) {

        request
            .get('https://todo.ly/api/filters/0/doneitems.json')
            .proxy('http://172.31.90.146:3128')
            .auth(account,password)
            .send()

            .end(function (err,res) {

                callback(err,res);
            });
}

exports.get = get;
exports.createItem = createItem;
exports.deleteItem = deleteItem;
exports.updateItem = updateItem;
exports.getItemFromFilter = getItemFromFilter;
exports.getListOfFilters = getListOfFilters;
exports.getDoneItems = getDoneItems;