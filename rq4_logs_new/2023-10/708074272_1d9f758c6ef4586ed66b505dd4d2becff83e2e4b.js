const express = require('express');
const app = express();
const port = 8000;

app.get("/",(req,res)=>{
   res.send('hello world');
});
app.get("/home",(req,res)=>{
   res.send('hello world home');
});
app.listen(port,()=>{
    console.log('all right');
})