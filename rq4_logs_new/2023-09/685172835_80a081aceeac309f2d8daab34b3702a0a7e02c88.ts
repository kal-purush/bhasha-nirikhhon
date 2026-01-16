const passport = require("passport");
const GoogleStrategy = require("passport-google-oauth20").Strategy;
import User from "../models/User";
import dotenv from "dotenv";
dotenv.config();

passport.use(
  new GoogleStrategy(
    {
      clientID: process.env.GOOGLE_CLIENT_ID,
      clientSecret: process.env.GOOGLE_CLIENT_SECRET,
      callbackURL: `${process.env.DEV_URL}/auth/google/callback`,
      passReqToCallback: true,
      scope: ["profile", "email"],
    },
    //@ts-ignore
    async function (req, accessToken, refreshToken, profile, cb) {
      console.log(
        "Google Auth Called with info",
        profile,
        accessToken,
        refreshToken
      );
      try {
        // Deferencing value from payload
        const { id, name, emails } = profile;
        const { familyName, givenName } = name;
        const userEmail =
          emails && profile.emails.length > 0 ? profile.emails[0].value : null;

        // Find existing User by googleId
        const existingUser = await User.findOne({ where: { googleId: id } });
        if (existingUser) {
          console.log("User found:", existingUser);
          return cb(null, existingUser);
        }

        // Create a new User with all required fields
        const newUser = await User.create({
          googleId: id,
          firstName: givenName,
          lastName: familyName,
          email: userEmail,
        });

        console.log("New Google User created", newUser);
        return cb(null, newUser);
      } catch (err) {
        console.error("Error creating Google user", err);
        return cb(err);
      }
    }
  )
);
// @ts-ignore
passport.serializeUser(function (user, done) {
  done(null, user.id);
});
// @ts-ignore
passport.deserializeUser(function (id, done) {
  // @ts-ignore
  User.findById(id, function (err, user) {
    done(err, user);
  });
});

export default passport;