const mongoose = require("mongoose");

const employeeSchema = mongoose.Schema({

    _id: mongoose.Schema.Types.ObjectId,
    name: String,
    salary: Number,
    dateOfJoining: Date,
    Aadhar: Number,
    description: String
  
  });
  
  module.exports = mongoose.model("employee", employeeSchema);
  