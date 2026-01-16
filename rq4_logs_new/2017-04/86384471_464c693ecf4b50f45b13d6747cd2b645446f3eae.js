'use strict';

var mongoose = require('mongoose');

var db = mongoose.connection;

db.on('error', function (error) {
  console.log(error)
});
db.once('open', function(){
  console.info(`Connected to mongo db, `);
});
mongoose.connect('mongodb://localhost/cursonode');