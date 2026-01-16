var mongodb = require('mongodb');
var server = new mongodb.Server('127.0.0.1',27017,{});
var client = new mongodb.Db('mydatabase',server,{w:1});