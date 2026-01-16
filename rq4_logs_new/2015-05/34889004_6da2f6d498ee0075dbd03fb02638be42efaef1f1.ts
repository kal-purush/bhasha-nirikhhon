var mongoose =require("mongoose");
module.exports = mongoose.model("Profile",{
  'email': {type: String, index:{unique:true},required: true},
  'user': {
    'prefix' : String,
    'firstname' : String,
    'lastname' : String,
    'birthday' : Date
  },
  'contact' : {
    'streetnumber' : Number,
    'apt' : Number,
    'streetname' : String,
    'city' : String,
    'province' : String,
    'country' : String,
    'postcode' : String,
    'phone1' : String,
    'phone2' : String,
    'email' : String
  },
  'payment' : {
    'holder' : String,
    'cardNumber' : String,
    'exp' : String,
    'cvv' : String,
  }
});