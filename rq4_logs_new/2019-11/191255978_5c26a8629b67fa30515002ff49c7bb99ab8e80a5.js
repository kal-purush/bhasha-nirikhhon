const mongoose = require('mongoose');
const Shema = mongoose.Schema;

let Event= new Shema({
    eventDate:{
        type:String
    },
    eventTime:{
        type:String
    },
    eventType:{
        type:String
    },
    details:{
        type:String
    },
    eventID:{
        type:Number
    },
    _id:{
        type:String
    }
});

module.exports= mongoose.model('event',Event);