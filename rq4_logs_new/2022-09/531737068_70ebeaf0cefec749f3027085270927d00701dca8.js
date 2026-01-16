require("dotenv").config();
const mongoose = require("mongoose");

mongoose.connect(
  "mongodb+srv://Mnewsholme:projects@cluster0.i2kxvgb.mongodb.net/Nailed_It",
  {
    useNewUrlParser: true,
    useUnifiedTopology: true,
  }
);

module.exports.Project = require("./projects");