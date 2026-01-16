const mongoose = require('mongoose');

const Billschema = mongoose.Schema(
    {
        Name : {
            type : String,
            required : true
        },
        Totalamount : {
            type : Number,
            required : true
        },
        Items : {
            type : Array,
            required : true
        }
      
    }
)

module.exports = mongoose.model("Bill",Billschema);