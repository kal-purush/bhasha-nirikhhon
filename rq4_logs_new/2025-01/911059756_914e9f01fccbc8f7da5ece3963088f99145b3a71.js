const express = require("express");
const cors = require("cors");
const dotenv = require("dotenv")

dotenv.config();


const app = express();

const PORT = process.env.PORT || 5001;

app.use(cors());

app.get("/" , (req,res)=>{
    res.send("Hello From Dockerized Backend!")
})

app.listen(PORT , ()=>{
    console.log(`Server running on port ${PORT}`)
})