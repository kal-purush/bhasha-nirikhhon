///////////////////////////////
// DEPENDENCIES
////////////////////////////////
//import dotenv to initilaize environmental variables for testing
// require('dotenv').config()

// pull PORT from .env, give default value of 4000
const mongoose = require('mongoose');
console.log
const {MONGODB_URI} = process.env

///////////////////////////////
// DATABASE CONNECTION
////////////////////////////////
mongoose.set('strictQuery', true);
mongoose.connect(MONGODB_URI)

// Connection Events
mongoose.connection
  .on("open", () => console.log("You are connected to mongoose"))
  .on("close", () => console.log("You are disconnected from mongoose"))
  .on("error", (error) => console.log(error));