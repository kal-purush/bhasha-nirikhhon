const express = require("express");
const app = express ();
const PORT = process.env.PORT || 8000;

// Test Rout
app.get('/',(req,res)=>res.send("Hello Mangala !!!"));

app.listen(PORT, ()=> console.log(`Server is running at port ${PORT}`));