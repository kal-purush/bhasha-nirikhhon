const express = required('express')
var home = express.Router();

home.get('/', (req, res)=>{
  res.render('broccoli.html');
});

home.get('/broccoli2', (req, res)=>{
  res.render('broccoli2.html');
})

module.exports = home;