/**
 * Created by lenovo on 30-05-2016.
 */
var fs = require('fs');
var Q = require('q');
var mongoose = require('mongoose');
mongoose.connect('mongodb://localhost/TE');
var db = mongoose.connection;

var services = {};

services.getMyAlert = getMyAlert;
services.getMyFavourites = getMyFavourites;
services.getMyPlan = getMyPlan;
services.getMyWorkList = getMyWorkList;

module.escape = services;

function getMyAlert() {
    var deferred = Q.defer();

    fs.readFile("../public/data/landing/myalert.json", function (err, data) {
        if (err) {
            deferred.reject()
        }
        deferred.resolve(data);
    });

    return deferred.promise;
}

function getMyFavourites() {
    var deferred = Q.defer();

    fs.readFile("../public/data/landing/myfavourites.json", function (err, data) {
        if (err) {
            deferred.reject()
        }
        deferred.resolve(data);
    });

    return deferred.promise;
}

function getMyWorkList() {
    var deferred = Q.defer();

    fs.readFile("../public/data/landing/myworklist.json", function (err, data) {
        if (err) {
            deferred.reject()
        }
        deferred.resolve(data);
    });

    return deferred.promise;
}

function getMyPlan() {
    var deferred = Q.defer();

    fs.readFile("../public/data/landing/myplans.json", function (err, data) {
        if (err) {
            deferred.reject()
        }
        deferred.resolve(data);
    });

    return deferred.promise;
}