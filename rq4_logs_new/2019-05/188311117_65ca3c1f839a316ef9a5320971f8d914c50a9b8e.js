const passport = require('passport')
var GoogleStrategy = require('passport-google-oauth20').Strategy;
var User = require('../models/user')

passport.use(new GoogleStrategy({
    clientID: process.env.GOOGLE_CLIENT_ID,
    clientSecret: process.env.GOOGLE_SECRET,
    callbackURL: process.env.GOOGLE_CALLBACK
    },
    function(accessToken, refreshToken, profile, cb) {
        // a user has logged in with OAuth...
        User.findOne({'googleId': profile.id}, (err, user)=> {
            if (err) return cb(err);
            if (user){
                //returning user
                cb(null, user);
            }else{
                //new user
                var newUser = new User({
                    name: profile.displayName,
                    email: profile.emails[0].value,
                    googleId: profile.id
                })
                newUser.save(err=>{
                    if (err) return cb(err)
                   return cb(null, newUser)
                })
            }
        })
    }
));

passport.serializeUser(function(user, done) {
    done(null, user.id);
});

passport.deserializeUser(function(id, done) {
    User.findById(id, function(err, user) {
      done(err, user);
    });
  });