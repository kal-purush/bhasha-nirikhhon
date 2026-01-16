const express = require('express');
const router = express.Router();
//////////////////////////////////////////////////////
const Dogs = require('../models/dog.js');


// Index Route
router.get('/', (req, res) => {
  Dogs.find({}, (err, foundDogs) => {
      res.render('dogs/index.ejs', {
        dogs: foundDogs
      });
  });

});



// New Route
router.get('/new', (req, res) => {
  res.render('dogs/new.ejs');
});

// 
router.get('/:id', (req, res) => {
  Dogs.findById(req.params.id, (err, foundDogs) => {
    res.render('dogs/show.ejs', {
      dogs: foundDogs
    });
  });
});

//
router.get('/:id/edit', (req, res) => {

  Dogs.findById(req.params.id, (err, foundDogs) => {
    res.render('dogs/edit.ejs', {
      dogs: foundDogs
    });
  });

});


//
router.put('/:id', (req, res) => {
  Dogs.findByIdAndUpdate(req.params.id, req.body, {new: true}, (err, updatedDogs)=> {
    console.log(updatedDogs, ' this is updatedDogs');
    res.redirect('/dogs');
  });
});
//
router.post('/', (req, res) => {
  console.log(req.body)
  Dogs.create(req.body, (err, createdDog) => {
    console.log(createdDog, ' this is the createdDog');
    res.redirect('/dogs');
  });

});



router.delete('/:id', (req, res) => {

  Dogs.findByIdAndRemove(req.params.id, (err, deletedDog) => {
    console.log(deletedDog, ' this is deletedDog');
    res.redirect('/dogs')
  })

});



module.exports = router;