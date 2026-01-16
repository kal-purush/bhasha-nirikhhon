// app/routes.js
import * as express from "express";
import * as mailgun from "mailgun-js";

export function mails(app: express.Express, authCheck: any, checkScopes: any) {

  // server routes ===========================================================
  // handle things like api calls
  // authentication routes
  app.use(function (req, res, next) {
    console.log(req.method, req.url);
    next();
  }
  );

  app.post('/api/mails/verification', authCheck, checkScopes, function (req, res) {
    var _id = req.params.id;
    console.log(req);
    console.log(process.env.api_key)
    console.log(process.env.DOMAIN)

    var api_key = process.env.api_key;
    var DOMAIN = process.env.DOMAIN;
    var mailgun = require('mailgun-js')({apiKey: api_key, domain: DOMAIN});
    
    var data = {
      from: 'Excited User <me@samples.mailgun.org>',
      to: 'yv.girard@gmail.com',
      subject: 'Hello',
      text: 'Testing some Mailgun awesomness!'
    };
    
    mailgun.messages().send(data, function (error, body) {
      console.log(error || body);
    });
    

    res.json({ info: 'error finding members', data: "Hello" });
  });
};