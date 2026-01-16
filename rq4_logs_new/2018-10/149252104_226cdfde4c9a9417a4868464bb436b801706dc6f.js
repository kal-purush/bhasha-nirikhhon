const passport = require('passport')
const LocalStrategy = require('passport-local').Strategy
const db = require('../model/db')

// setup passport.js
passport.use(new LocalStrategy(
    async (username, password, done) => {
      const userData = await db.user.getValidUserid(username, password, db.exposedInstance)
      const unwrappedUserData = userData[0]
      console.log("JSON OBJECT: " + JSON.stringify(unwrappedUserData))

      if (unwrappedUserData) {
        return done(null, unwrappedUserData.userid)
      } else {
        return done(userData, false)
      }
    }
))

// No actual serialisation / deserialisation is done
// The following are dummy methods
passport.serializeUser(function(user, done) {
      done(null, user)
    }
)

passport.deserializeUser(function(id, done) {
        done(null, id)
    }
)

module.exports = passport