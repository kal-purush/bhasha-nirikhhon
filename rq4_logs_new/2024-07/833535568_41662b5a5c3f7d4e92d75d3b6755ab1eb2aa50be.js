const express = require('express');
const path = require('path');
const app = express();

// app.use(express.static(path.join('./static/menu')));
// app.use(express.static(path.join('./static/home')));

app.use(express.static(path.join(__dirname, './static')));



app.get("/home", (req,res) =>{
    res.status(200).sendFile(path.resolve('./static/home/home.html'));
});

app.get("/menu", (req,res) =>{
    res.status(200).sendFile(path.resolve('./static/menu/menu.html'));
});



const port = 8000;
app.listen(port, ()=>{
    console.log(`Server started on port ${port}`)
})

