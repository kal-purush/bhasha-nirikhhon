/// <reference path="./typings/node/node.d.ts"/>
/// <reference path="./typings/passport/passport.d.ts"/>

/**
 * Created by Dominic Böttger on 10.06.2015
 * INSPIRATIONlabs GmbH
 * http://www.inspirationlabs.com
 */
var wsfedsaml2 = require('passport-wsfed-saml2').Strategy;
var _ = require('lodash');

var molecuel;

class mlcl_auth_saml2 {
  molecuel:any;
  mlcl_user:any;
  usermodel:any;
  passport:any;
  jwt:any;
  app:any;

  constructor() {
    this.molecuel = molecuel;
    this.molecuel.on('mlcl::user::init:post', (usermodule:any) => {
      this.mlcl_user = usermodule;
      this.usermodel = usermodule.model;
      this.passport = usermodule.passport;
      this.jwt = usermodule.jwt;
      this.registerPassportfunction();
    });
  }
  registerPassportfunction() {
    if(this.molecuel.config.user.authtypes && this.molecuel.config.user.authtypes.saml2) {
      var conf = this.molecuel.config.user;
      this.passport.use(new wsfedsaml2(
      {
        path: '/login/saml2/callback',
        realm: conf.authtypes.saml2.realm,
        identityProviderUrl: conf.authtypes.saml2.identityProviderUrl,
        protocol: 'samlp',
        cert: conf.authtypes.saml2.cert
      },(profile, done) => {
        // @todo Check if the user is already in the database
        // use mappings?
        let unamefield = 'username';
        if(conf.authtypes.saml2.fieldmappings && conf.authtypes.saml2.fieldmappings.username) {
          unamefield = conf.authtypes.saml2.fieldmappings.username;
        }
        let susername = profile[unamefield];
        this.usermodel.findOne({ username: susername}, (err, doc) => {
          let userfieldmapping = (user) => {
            if(conf.authtypes.saml2.fieldmappings) {
              let reverseMappings = {};
              _.each(Object.keys(conf.authtypes.saml2.fieldmappings), (fieldname) => {
                if(!user.username && fieldname !== 'username') {
                  reverseMappings[conf.authtypes.saml2.fieldmappings[fieldname]] = fieldname;
                }
              });
              _.each(reverseMappings, (fieldname) => {
                _.set(user, fieldname, profile[conf.authtypes.saml2.fieldmappings[fieldname]]);
              });
            } else {
              // use predefined fieldmappings (this possibly fails)
              if(!user.username) {
                user.username = profile.username;
              }
              user.name.first = profile.firstname;
              user.name.last = profile.lastname;
              user.email = profile.email;
            }
          }
          if(doc) {
            userfieldmapping(doc);
            doc.save(function(err) {
              done(null, doc);
            });
          } else {
            let user = new this.usermodel();
            user.authtype = 'saml2';
            userfieldmapping(user);
            user.save(function(err) {
              done(err, user);
            });
          }
        });
      //  this.usermodel.find
        // add the specific data to the user with a pre save handler
      }));
    }
  }

  middleware(config, app, mod) {
    let usermodule = molecuel.modules.user;
    let passport = usermodule.module.passport;
    app.get('/login/saml2',
      passport.authenticate('wsfed-saml2', { failureRedirect: '/', failureFlash: false }),
      function(req, res) {
        res.redirect('/');
      }
    );
    app.post('/login/saml2/callback',
      passport.authenticate('wsfed-saml2', { failureRedirect: '/', failureFlash: false }),
      function(req, res) {
        console.log(req.user);
        res.status(200).send('\
          <html> \
            <head></head> \
            <body> \
              <script> \
                localStorage.setItem(\'userdata\', '+ usermodule.getTokenFromRequest(req) + ') \
                console.log(localStorage.getItem(\'userdata\')); \
              </script> \
            </body> \
          </html>');
//        res.redirect('/');
      }
    );
  }
}


/* ************************************************************************
 SINGLETON CLASS DEFINITION
 ************************************************************************ */
var instance = null;

var getInstance = function(){
  return instance || (instance = new mlcl_auth_saml2());
};

var init = function (m) {
  molecuel = m;
  // store molecuel instance
  return getInstance();
};

module.exports = init;