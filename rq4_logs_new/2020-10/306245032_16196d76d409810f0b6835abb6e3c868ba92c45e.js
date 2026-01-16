var mongoose = require('mongoose');
var passport = require('passport');
var User = mongoose.model('User');

// Register user
register = (req, res) => {
  var user = new User();
  user.firstname = req.body.firstname;
  user.email = req.body.email;
  user.lastname = req.body.lastname;

  user.setPassword(req.body.password);

  if (user.firstname && user.lastname) {
    user.save(function (err) {
      if (err) {
        if (11000 === err.code || 11001 === err.code) {
          res.status(404).json({
            message: "User already registered with this email"
          });
          return;
        }
      }
      res.status(200);
      res.json({
        success: true
      });
    });
  } else {
    res.status(404).json({
      message: "missing firstname or lastname"
    });
  }

};

// Login approval
login = (req, res) => {
  passport.authenticate('local', function (err, user, info) {

    // If Passport throws/catches an error
    if (err) {
      res.status(404).json(err);
      return;
    }

    // Return that user is found
    if (user) {
      res.status(200).json({
        success: true
      });
    } else {
      // If user is not found
      res.status(401).json(info);
    }
  })(req, res);

};

module.exports = {
  register,
  login
}