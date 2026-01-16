const express = require("express");
const { initDB } = require("./dbConfig");
const app = express();
require('dotenv').config()
const router = require("./routes/user")
const port = 9000


app.use(express.json())
app.use(express.urlencoded({extended:true}))
app.use(express.static('public'))


app.use("/",router)

initDB();


app.listen(port,()=>{
    console.log(`server has started ${port}`)
})