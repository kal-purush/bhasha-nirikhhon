const MongoClient = require('mongodb').MongoClient;
const uri = "mongodb+srv://xiangxinyue:xiangxinyue@decision-ha1xk.mongodb.net/test?retryWrites=true&w=majority";
const client = new MongoClient(uri, { useNewUrlParser: true, useUnifiedTopology: true });

module.exports = client;