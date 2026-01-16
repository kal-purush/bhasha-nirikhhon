const mysql = require('mysql');
const express = require('express');
var app = express();
const bodyparser = require('body-parser');

const sampledata = require('./mock-data/sample-data');


app.use(bodyparser.json());

const port = 3000;

// enter your connection details here
var mysqlConnection = mysql.createConnection({
    host: 'localhost',
    user: 'root',
    password: 'Inseyab@123',
    database: 'world'
});

// initial connection to MySQL Database
mysqlConnection.connect(
    err => {
        if (!err) {
            console.log("DB connection succeeded!");
        } else {
            console.log("DB connection failed!\n Error: " + JSON.stringify(err, undefined, 2));
        }
    }
);

// configuring port for Express server
app.listen(port, () => console.log('Express server is running at port ' + port));



// get sample data from mock file
app.get('/sample', (req, res) => {
    res.send(sampledata);
});

// get all countries data
app.get('/countries', (req, res) => {
    mysqlConnection.query('SELECT * FROM country', (err, rows, fields) => {
        if (!err) {
            res.send(rows);
        } else {
            console.log("Error: " + JSON.stringify(err, undefined, 2));
        }
    })
});

// get single country data
app.get('/countries/:id', (req, res) => {
    mysqlConnection.query('SELECT * FROM country WHERE Code = ?', [req.params.id], (err, rows, fields) => {
        if (!err) {
            res.send(rows);
        } else {
            console.log("Error: " + JSON.stringify(err, undefined, 2));
        }
    })
});