const mongoose = require('mongoose');

const jobPostSchema = new mongoose.Schema({
  title: {
    type: String,
    required: true
  },
 company: {
    type: String,
    required: true
  },

  joblocation: {
  type:String,
  required:true,
  },
  basesalary: {
   type:String,
   required:true,
  },
 recruiter:{
    type:mongoose.Schema.Types.ObjectId,
    ref:"Recruiter",
},

appliedCandidates:[{
  type:mongoose.Schema.Types.ObjectId,
  ref:"Employee",
}]

});

const jobPost = mongoose.model('jobPost', jobPostSchema);

module.exports = jobPost;