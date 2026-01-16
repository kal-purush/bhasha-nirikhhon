///<reference path='collections/collections.d.ts' />
///<reference path='node/node.d.ts' />
///<reference path='node/express.d.ts' />
///<reference path='mraa.d.ts' />

import fs = require("fs")
import http = require("http")
import path = require("path")
import mraa= require("mraa")
import express = require("express")
import index = require("./routes/index")
import user = require("./routes/user")

var col = fs.readFileSync('./collections/collections.js','utf8');
eval(col);

var app = express();
console.log(mraa.getVersion())


app.configure(function(){
  app.set('port', process.env.PORT || 3000);
//  app.set('views', __dirname + '/views');
    app.set('views', __dirname + '/views');
    app.set('view engine', 'jade');
    app.set('view options', { layout: false });
    app.use(express.bodyParser());
    app.use(express.methodOverride());
    app.use(express.static(__dirname + '/public'));
});

app.configure('development', function(){
    app.use(express.errorHandler({ dumpExceptions: true, showStack: true }));
});

app.configure('production', function(){
    app.use(express.errorHandler());
});

app.get('/', index.index);
app.get('/users', user.list);
var x = new collections.Set<number>();
x.add(123);
x.add(123);
class Person{
     name: String;
    firstName :String;
    test1: collections.Set<String>;
    res:any
    constructor(){
        this.test1 = new collections.Set<String>()

    }
    toString() {
        return collections.makeString(this);
    }


    toJson() {
        return collections.makeJson(this)
    }
}

app.get('/user/:userid', function(req, res) {
    console.log('getting user ' + req.params.userid);

    var o = new Person
    o.name = 'hello'
    o.firstName = 'barais'
    o.test1.add("tutu")
    o.test1.add("titi")
    res.json(JSON.parse(o.toJson()));

});

app.listen(3000, function(){
    console.log("Demo Express server listening on port %d in %s mode", 3000, app.settings.env);
});