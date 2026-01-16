import * as express from 'express';
import * as bodyParser from 'body-parser';
import { existsSync } from 'fs';
import Database from 'better-sqlite3';

const app = express();

app.use(bodyParser.urlencoded({ extended: true }));

const dbFile = './.data/sqlite.db';
const exists = existsSync(dbFile);
const sqlite3 = require('sqlite3').verbose();

const db = new sqlite3.Database(dbFile);

db.serialize(() => {
  if (!exists) {
    db.run(`
      CREATE TABLE Dreams (dream TEXT)
    `);
    console.log('New table Dreams created!');
    
    db.serialize(() => {
      db.run(`
        INSERT INTO Dreams (dream) 
               VALUES ("Find and count some sheep"), 
                      ("Climb a really tall mountain"),
                      ("Wash the dishes")
      `);
    });
  }
  else {
    console.log('Database "Dreams" ready to go!');
    db.each('SELECT * from Dreams', function(err, row) {
      if ( row ) {
        console.log('record:', row);
      }
    });
  }
});

app.get('/getDreams', function(request, response) {
  db.all('SELECT * from Dreams', function(err, rows) {
    response.send(JSON.stringify(rows));
  });
});

const listener = app.listen(process.env.PORT, () => {
  console.log('Your app is listening on port ' + listener.address().port);
});