const express = require('express');
const path = require('path');
var bodyParser = require('body-parser');

// Following the "Single query" approach from: https://node-postgres.com/features/pooling#single-query

const { Pool } = require('pg'); // This is the postgres database connection module.

// This says to use the connection string from the environment variable, if it is there,
// otherwise, it will use a connection string that refers to a local postgres DB
const connectionString = process.env.DATABASE_URL;

// Establish a new connection to the data source specified the connection string.
const pool = new Pool({connectionString: connectionString, ssl: true});


let app = express()
  .set('port', (process.env.PORT || 5000))
  .use(express.static(path.join(__dirname, 'public')))
  .set('views', path.join(__dirname, 'views'))
  .set('view engine', 'ejs')
  .get('/', (req, res) => {
    res.render('generator');
  });

app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.json());

app.get('/community', function(req, res) {
  getCommunity(function(err, result) {
    if (err) {
      res.status(400);
      return res.send();
    }
    res.json(result.rows);
//    console.log(result.rows);
  });
});

function getCommunity(callback) {
  var sql = "SELECT * FROM characters";
  pool.query(sql, callback);
}

// CREATE
//function createCharacter() {
app.post('/character', function(req, res) {
  let name = req.body.name;
  let role = req.body.role;
  let age = req.body.age;
  let gender = req.body.gender;
  let race = req.body.race;
  let nationality = req.body.nationality;

  let sql = "INSERT INTO characters (name, role, age, gender, race, nationality) VALUES ($1, $2, $3, $4, $5, $6)";
  let params = [name, role, age, gender, race, nationality];

  pool.query(sql, params, function(err, result) {
    if (err) {
      console.log(err);
      res.status(400);
    }
    else {
      res.status(201);
    }
    res.end();
  });
});
//}


// UPDATE
app.patch('/character/:id', function(req, res) {
  let id = req.body.id;

  let name = req.body.name;
  let role = req.body.role;
  let age = req.body.age;
  let gender = req.body.gender;
  let race = req.body.race;
  let nationality = req.body.nationality;

  let sql = "UPDATE characters SET name=COALESCE($1, name), role=COALESCE($2, role), age=COALESCE($3, age), gender=COALESCE($4, gender), race=COALESCE($5, race), nationality=COALESCE($6, nationality) WHERE id=$7";

  let params = [name, role, age, gender, race, nationality, id];

//  console.log(params);

  pool.query(sql, params, function(err, result) {
    if (err) {
      console.log(err);
      res.status(400);
    }
    else {
      let status = res.status(204);
      console.log(status);
    }
    res.end();
  });
});

app.get('/character/:id', function(req, res) {
  let id = req.params.id;

  let sql = "SELECT * FROM characters WHERE id = $1";

  let params = [id];

  pool.query(sql, params, function(err, result) {
    if (err) {
      res.status(400);
      return res.send();
    }

    let character = result.rows.shift();

    console.log(character);

    res.json(character);

//    res.json(result.rows.shift());
  });
});

// DELETE
app.delete('/character/:id', function(req, res) {
  let id = req.params.id;

  let sql = "DELETE FROM characters WHERE id=$1";
  let params = [id];

  pool.query(sql, params, function(err, result) {
    if (err) {
      console.log(err);
      res.status(400);
    }
    else {
      res.status(202);
    }
    res.end();
  });
});



// Start the server running
app.listen(app.get('port'), function() {
  console.log('Node app is running on port', app.get('port'));
});