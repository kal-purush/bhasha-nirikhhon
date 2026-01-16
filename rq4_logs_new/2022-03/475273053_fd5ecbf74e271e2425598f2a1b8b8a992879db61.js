const express = require('express');
const morgan = require('morgan');
const mongoose = require('mongoose');
const blogRoutes = require('./routes/blogRoutes');

const secure = require('./secure');


// connect to MongoDB
const dbURI = secure.mongoAccess
mongoose.connect(dbURI, { useNewUrlParser: true, useUnifiedTopology: true })
.then((result) => app.listen(3000))
.then((result) => console.log('connected to db'))  
// .then(result => app.listen(3000))
  .catch(err => console.log(err));

// express app
const app = express();

// listen for requests

// middleware and static files
app.use(express.static('public'));
app.use(express.urlencoded({ extended: true }));
app.use(morgan('dev'));



// register view engine
app.set('view engine', 'ejs');
// app.set('views', 'myviews');

// main routes
app.get('/', (req, res) => {
  res.redirect('/blogs');
});

app.get('/about', (req, res) => {
  res.render('about', { title: 'About' });
});

// blog routes
app.use('/blogs', blogRoutes);

// 404 page
app.use((req, res) => {
  res.status(404).render('404', { title: '404' });
}); 