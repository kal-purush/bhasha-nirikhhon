var events=require('events');
var logger= require("../logger");

var emitter = new events.EventEmitter();

emitter.on("update",function(data){
    logger.info("Number of employees in"+data+" is updated")

});
emitter.on("more",function(data){

logger.info("For pattern serach"+"the number of records exceeded 50 and it is"+data);
});

module.exports=emitter;