//importing the express module
const express=require('express');
//importing mongoose
require('dotenv').config();
const mongoose=require('mongoose');
const authRouter=require("./routes/auth")
//defined the port number the server will listen on
const PORT =3000;
//creating the instance of a express application
//because it gives us the starting point

const app=express();
// middleware- to register routes or to mount routes
//database url
const db=process.env.db;
mongoose.connect(db) .then(()=>{
    console.log("connected sucessfully");
});
app.use(express.json());
app.use(authRouter);
const cors = require("cors");
app.use(cors());


//start the server and listen on the specified port
app.listen(PORT,"0.0.0.0",function(){
    // log the number
    console.log(`server is running on port ${PORT}`)
});