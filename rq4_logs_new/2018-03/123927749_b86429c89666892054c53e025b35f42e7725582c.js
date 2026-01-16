const express = require('express');
const mongoose = require('mongoose');
const bodyParser = require('body-parser');
const session = require('express-session');
const MongoStore = require('connect-mongo')(session);

const User = require('./routes/User');
const Airport = require('./routes/Airport');

const app = express();
const port = process.env.PORT || 3000;

mongoose.connect('mongodb://adm:ruslan16@ds163494.mlab.com:63494/luft')
  .then(() => {
    console.log('Database connection set');
  })
  .catch((error) => {
    console.log('Database connection failed.');
    console.log(error);
  });

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));
app.use(session({
  secret: 'w23n4b5bh3ch5f83498543yfch3jkhcfjkh48cf438c7f',
  store: new MongoStore({ mongooseConnection: mongoose.connection }),
  resave: false,
  saveUninitialized: true,
}));

app.use('/users', User);
app.use('/airports', Airport);

app.listen(port);