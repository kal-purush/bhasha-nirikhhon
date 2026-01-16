import * as express from 'express';

export class Server {
    private app: express.Express;
    private port = process.env.PORT || 8080;

    constructor() {
        this.app = express();
    }

    startServer() {
        this.app.use(express.static('app'));
        this.app.use(express.static('.'));

        this.app.listen(this.port);

        console.log("Server started with port", this.port);
    }
}


//var express = require('express');
//var app = express();
//var port = process.env.PORT || 8080;
//var mongo = require('./backend/mongo');
//
//app.use(express.static('app'));
//app.use(express.static('.'));
//
//app.listen(port);
//
//console.log("Server started with port", port);

//mongo.connectMongo();
//var propositionsCollection = mongo.getCollection('propositions');

//mongo.insertDocument(propositionsCollection,{name:'test'});
