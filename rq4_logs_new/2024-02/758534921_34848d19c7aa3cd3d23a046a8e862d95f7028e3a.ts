import { Strategy as GoogleStrategy } from 'passport-google-oauth20'
import passport from 'passport'
import dotenv from 'dotenv'

dotenv.config()

const clientID: string = process.env.GOOGLE_CLIENT_ID ?? 'defaultClientId'
const clientSecret: string = process.env.GOOGLE_CLIENT_SECRET ?? 'defaultClientSecret'

passport.use(
  <passport.Strategy>new GoogleStrategy(
    {
      clientID: clientID,
      clientSecret: clientSecret,
      callbackURL: '/auth/google/callback',
      scope: ['profile', 'email'],
    },
    function (accessToken, refreshToken, profile, callback) {
      callback(null, profile)
    },
  ),
)

passport.serializeUser((user: Express.User, done) => {
  done(null, user)
})

passport.deserializeUser((user: Express.User, done) => {
  done(null, user)
})