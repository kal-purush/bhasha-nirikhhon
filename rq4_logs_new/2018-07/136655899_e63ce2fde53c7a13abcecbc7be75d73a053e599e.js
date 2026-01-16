const mongoose = require('mongoose')
const Movie = require('../../models/movie')

module.exports = (req, res) => {
  Movie.find({}, (err, movies) => {
    if (err)
      res.send(err)
    res.json(movies)
  })
}