/**
 * Created by fish on 2015/4/3.
 */

///<reference path="typings/node/node.d.ts" />
var Client = require("umpay-transfer");

var options = {
    funCode:"xxxxx",
    spId:"xxxxx",
    userId:"xxxxx",
    host:"xxxxx",
    port:"xxxxx",
    verifyRespSign:false,
    desKeyPath:"xxxxx/xxxxx.dat",
    priKeyPath:"xxxxx/xxxxx.pri",
    pubKeyPath:"xxxxx/xxxxx.pub"
};

var client = new Client(options);

client.transfer("136xxxxxxxxx", 500).then(function (rst){
    console.log("transfer success");
}).catch(function (err){
    console.log(err)
});