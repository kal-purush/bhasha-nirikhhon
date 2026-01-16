const express = require('express');
const path = require('path');

const app = express();

const PORT = process.env.port || 3000;

// Middleware for parsing JSON and urlencoded form data
app.use(express.json());
app.use(express.urlencoded({ extended: true }));


app.get('/', (req, res) =>
    res.sendFile(path.join(__dirname, './public/index.html'))
);
app.get('/notes', (req, res) =>
    res.sendFile(path.join(__dirname, './public/notes.html'))
);



app.listen('3000', (req, res) => console.log(`Listening at PORT ${PORT}.`));