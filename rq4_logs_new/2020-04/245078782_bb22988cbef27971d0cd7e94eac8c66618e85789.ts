import { Model, model } from 'mongoose';
import passport from 'passport'
import * as passportLocal from 'passport-local'

import { IUserModel } from '../models/interface'

const LocalStrategy = passportLocal.Strategy

const Users: Model<IUserModel> = model('Users');

passport.use(
  new LocalStrategy(
    {
      usernameField: 'user[email]',
      passwordField: 'user[password]',
    },
    (email: string, password: string, done) => {
      Users.findOne({ email })
        .then(user => {
          if (!user || !user.validatePassword(password)) {
            return done('email or password is invalid', false)
          }

          return done(null, user);
        })
        .catch(done);
    }
  )
);