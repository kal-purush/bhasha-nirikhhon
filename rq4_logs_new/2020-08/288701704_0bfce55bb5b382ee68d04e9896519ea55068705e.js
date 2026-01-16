var mongoose = require('mongoose');
var Schema = mongoose.Schema;
autoIncrement = require("mongoose-auto-increment");
var connection = mongoose.createConnection("mongodb://localhost/test");
autoIncrement.initialize(connection);


var Test= new Schema({
testname: {type:String,
    required:true,
    
    },
testlecturer: {type:String,
        required:false,
},
courseid: {type:Number,
    required:true,
},
testid: {type:Number
},
marksObtained: { type: Number } ,
  
  
file:{type:String,
  required:false,
},

});

Test.plugin(autoIncrement.plugin, {
  model: "Test",
  field: "testid",
  startAt: 1000,
});
var Test = mongoose.model('Test', Test );
module.exports = Test;