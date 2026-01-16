var mongoose =require("mongoose");
module.exports = mongoose.model("Subscription",{
  'email': {type: String, index:{unique:true},required: true},
  'choice': String,
  'like' : [String],
  'dislike':[String],
  'comment':String
});