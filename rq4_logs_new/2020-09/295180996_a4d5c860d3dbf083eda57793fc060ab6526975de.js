/* ====================
       Assignment 1 
===================== */

const express = require('express');

const app = express();

app.get('/', (req, res) => {
    res.send('<h1>Hello, My Server!');
});


app.listen(3000, () => {
    console.log('Server started on port 3000');
});