const express = require('express');
const path = require('path');
const app = express();
const port = 3000;


app.use(express.static('public', { index: 'index.html' }));


app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});


app.listen(3000);