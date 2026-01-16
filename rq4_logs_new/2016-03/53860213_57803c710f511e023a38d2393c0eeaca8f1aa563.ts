import { User } from '../contracts/app.contract';

import passport = require('passport');
import jwt = require('jwt-simple');
import async = require('async');
import bcrypt = require('bcrypt');
import crypto = require('crypto');
import moment = require('moment');

import UserService = require('../helpers/userService');
import MailService = require('../helpers/mailService');

var AppConfig = require('../config/app');
var AuthConfig = require('../config/auth');

/**
 * The export section is necessary to make the functions available to the
 * Swagger generated routes.
 */
export = {
  register: Authentication.register,
  login: Authentication.authenticate,
  logout: Authentication.logout,
  forgotPassword: Authentication.forgotPassword,
  resetPassword: Authentication.resetPassword,
  validateResetToken: Authentication.validateResetToken
};

/**
 * The authentication controller handling the auth routes.
 */
module Authentication {

  /**
   * Authenticate a user with the local strategy.
   *
   * @param req
   * @param res
   * @param next
   */
  export function authenticate(req, res, next) {

    passport.authenticate('local-login', { session: true },
      function (err, user) {

        if(err || !user)
          return res.status(401).send('status.user.error.authorization.failure');

        req.logIn(user, function(err) {
          if (err)
            return res.status(401).send('status.user.error.authorization.failure');

          var token =
            getAuthenticationToken(req, res, AuthConfig['secret']);

          if (!token)
            return res.status(401).send('status.user.error.authorization.failure');

          UserService
            .getProfile(user.id)
            .then(function (user) {
              if (user) {
                user['token'] = token;
                res.status(200).json(user);
              }
              else {
                res.status(500).send({
                  message: 'Could not retrieve user profile from database.'
                });
              }
            })
            .catch(function(err) {
              res.status(500).json({ message: err.message });
            });
        });
      })(req, res, next);
  }

  /**
   * The user forgot his password and requested a password reset.
   *
   * @param req
   * @param res
   */
  export function forgotPassword(req, res) {

    var languageKey = req.headers['x-language'];

    async.waterfall([
      function(done) {
        UserService.getUserFromEmail(req.body.email, done);
      },

      function(user, done) {
        crypto.randomBytes(20, function(err, buf) {
          var token = buf.toString('hex');
          done(err, token, user)
        })
      },

      function(token, user, done) {
        var expirationTime = Date.now() + 24 * 3600000; // 1 day
        var expirationDate = moment(expirationTime).format('YYYY-MM-DD HH:mm:ss');

        UserService.addResetToken(user, token, expirationDate, done);
      },

      function(token, user, done) {
        MailService.sendPasswordResetLink(user.email, token, req.headers.origin, languageKey, done);
      }
    ], function(err) {
      if (err)
        return res.status(500).json({ message: err.message });

      res.sendStatus(200);
    });
  }

  /**
   * Create authentication token from user id.
   *
   * @param req
   * @param res
   * @param secret
   * @returns {any}
   */
  function getAuthenticationToken(req, res, secret) {

    if (req.user) {
      var expires = undefined;

      var now = new Date();

      var validity = AppConfig['validityPeriod'];
      if (typeof validity == "number" && isFinite(validity) && validity % 1 === 0) {
        now.setDate(now.getDate() + validity);
        expires = now.getTime();
      }
      else
        throw new Error('Wrong configuration format.');

      return jwt.encode({ iss: req.user.id, exp: expires }, secret);
    }
    return null;
  }

  /**
   * Reset the user password and send confirmation mail.
   *
   * @param req
   * @param res
   */
  export function resetPassword(req, res) {

    var languageKey = req.headers['x-language'];

    async.waterfall([
      function(done) {
        UserService
          .getUserFromPasswordResetToken(req.body.resetToken, done);
      },
      function(user, done) {
        var salt = bcrypt.genSaltSync(10);
        var password = bcrypt.hashSync(req.body.newPassword, salt);

        UserService.resetUserPassword(user, password, done)
      },
      function(user, done) {
        MailService.sendPasswordResetConfirmation(user.email, languageKey, done);
      }
    ], function(err) {
      if (err)
        return res.status(500).json({ message: err.message });

      res.sendStatus(200);
    });
  }

  /**
   * Remove user object from request object.
   *
   * @param req
   * @param res
   */
  export function logout(req, res) {
    req.logout();

    // TODO invalidate API token (?)
    // If we invalidate a token, we must persist this token to be able to
    // compare tokens of incoming requests tokens with the invalidated ones.
    // We could use a Redit store for that purpose.

    res.sendStatus(200);
  }

  /**
   * Register the new user and authenticate him with Passport.
   *
   * @param req
   * @param res
   * @param next
   */
  export function register(req, res, next) {

    passport.authenticate('local-signup', { session: true },
      function (err, user, info) {

        if (err)
          return res.status(500).json({ message: err.message });

        if (!user) {
          var message = info && info.message ?
            info.message : "status.user.error.signup.failure";

          return res.status(401).send(message);
        }

        var token =
          getAuthenticationToken(req, res, AuthConfig['secret']);

        if (!token)
          return res.status(401).send('status.user.error.authorization.failure');

        UserService
          .getProfile(user.id)
          .then(function (user:User) {
            if (user) {
              user.token = token;
              res.status(200).json(user);
            }
            else {
              res.status(500).send({
                message: 'Could not retrieve user profile from database.'
              });
            }
          })
          .catch(function (err) {
            res.status(500).json({ message: err.message });
          });
      })(req, res, next);
  }

  /**
   * Validates the given reset token by trying to extract the user object.
   *
   * @param req
   * @param res
   */
  export function validateResetToken(req, res) {

    UserService.getUserFromPasswordResetToken(req.params['resetToken'], function(err) {
      if(err)
        res.status(500).json({ message: err.message });

      res.sendStatus(200);
    });
  }
}