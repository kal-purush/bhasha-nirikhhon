// getting all movies
var express = require('express')
var router = express.Router()

// setup movie model
var Movie = require('../models/movie')

router.get('/movies', function (req, res) {
  Movie.find({}, function (err, allMovies) {
    if (err) res.send(err)
    res.send(allMovies)
  })
})

// creating new movies
router.post('/movies', function (req, res, next) {
  // create movie when we receive the post request
  var newMovie = new Movie(req.body)
  newMovie.save(function (err, newMovie) {
    if (err) res.send(err)
    res.send(newMovie)
  })
})

module.exports = router