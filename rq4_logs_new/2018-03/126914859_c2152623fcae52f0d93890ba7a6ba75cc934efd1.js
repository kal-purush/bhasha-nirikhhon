var fs = require('fs');
var mongodbClient = require('mongodb').MongoClient;
var dbClient;
mongodbClient.connect("mongodb://localhost:85/test",{native_parser:true},fucntion(err,db){
  if(!err){
    console.log("Established Mongo connection");
    dbClient = db;
  }else{
    console.log("Error establishing connection",err);
  }
});

var kafka = require('kafka-node'),
    client  = new kafka.Client(),
    Consumer = new kafka.ConsumerStream,
    consumer = new Consumer(
      client,
      [{topic:'recipe_A-H',partition:0},{topic:'recipe_I_P', partition:1},{topic:'recipe_Q_P'}]
    );

consumer.on('message',function(message){
  loadmessage(message).then(function(){
    console.log("loading");
  });
});

consumer.on('error',function(err){
  console.console.log("Encountered error",err);
});

var loadMessage = function(message,callback){
  var doc = JSON.parse(message.value);
  try_update_collection(message).then(function(data){
    console.log("updated the collection");
    console.log(data);
    callback();
  });
}

var try_update_collection = function(message){
  return new Promise(function(resolve,reject){
      db.collection('test').update(
        {id:message.id},
        message,
        {upsert:true}
      ),function(err,result){
        if (!err){
          resolve(result);
        }else{
          reject();
        }
      }
    }
}