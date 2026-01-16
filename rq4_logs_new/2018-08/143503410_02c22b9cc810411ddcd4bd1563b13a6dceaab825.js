const mysql = require('mysql');

const express = require('express');
const morgan = require('morgan');
const path = require('path');

var app = express();
app.use(morgan('combined'));


// to get the data to add
app.get('/addMarker/:text', function(req, res) {
    var markerLoc = req.params.text.split(",");
    res.send(markerLoc[0], markerLoc[1]);
    //console.log(markerLoc[0], markerLoc[1], markerLoc[2])
    //addMarker(markerLoc[0], markerLoc[1], markerLoc[2]);
})

// to retrieve data
app.get('/getMarker', function(req, res) {
    connection.query('select * from new_markers',(err, row) => {
        if(err) throw err;
        res.send(JSON.stringify(row));
      });
})

//  Connecting to the database
const connection = mysql.createConnection({
    host:'localhost',
    user: 'user',
    password: 'password',
    database: 'cleanspaceregistry'
});

connection.connect((err) => {
    if(err) throw err;
    console.log('connected');
})


// add value to the database
function addMarker(lat, lng, text) {
    connection.query('INSERT INTO new_markers VALUES(DEFAULT,' + lat + ',' + lng + ',' + text + ')',(err, res) => {
        if(err) throw err;
        console.log('Last insert ID:', res.insertId);
      });
}


/*


connection.query('INSERT INTO markers VALUES(DEFAULT, 12.3334, 12.3424)',(err, res) => {
  if(err) throw err;

  console.log('Last insert ID:', res.insertId);
});

//get methods
app.get('/data', function (req, res) {
    res.send(data);
  });

*/

app.listen(3000, function () {
    console.log(`IMAD course app listening on port 3000!`);

  });
