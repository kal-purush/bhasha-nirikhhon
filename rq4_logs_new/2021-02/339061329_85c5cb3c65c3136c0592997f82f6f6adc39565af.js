const express = require('express');
const router = express.Router();

// Home route
router.get('/', isLoggedIn, (req, res) => {
  res.render('index', {
    name: req.user.name
  });
});

function isLoggedIn(req, res, next) {
  if (req.isAuthenticated()) {
    return next();
  }

  res.redirect('/login');
}

module.exports = router;