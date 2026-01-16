const express=require("express")
const mongoclient=require("mongoose")
const Dotenv=require("dotenv")
const app=express();

Dotenv.config();
const PORT=5566;
mongoclient.connect(process.env.MONGOURL).then(()=>{
    console.log("Database connected successfully")
})

app.listen(PORT,()=>{
    console.log(`My Server is Running on ${PORT} number`)
})