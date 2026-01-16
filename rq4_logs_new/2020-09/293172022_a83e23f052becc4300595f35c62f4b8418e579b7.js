const express = require('express');
const router = express.Router();
const sgMail = require('@sendgrid/mail');

sgMail.setApiKey(process.env.SENDGRID_API_KEY);

router.post('/', (req, res, next) => {
  const query = req.query;
  const body = req.body;
  const bodySize = Object.keys(body).length;
  if (bodySize !== 0 && Object.keys(query).length !== 0) {
    res.status(422).json({
      error: 'Do not include both a body and query.'
    });
  }
  let msg = {};
  if (bodySize > 0) {
    if (!body.message) {
      return res.status(400).json({
        error: 'message required'
      });
    } else if (!body.email) {
      return res.status(400).json({
        error: 'sender email required in field "email"'
      });
    }
    msg = {
      to: 'david@whealetech.com',
      from: 'contactme@davidwheale.com',
      subject: 'Contact from davidwheale.com',
      text: body.message,
      reply_to: body.email
    };
    delete body.email;
    delete body.message;
    for (const field in body) {
      msg.text = msg.text.concat(`\n${ field }: ${ body[field] }`);
    }
  } else {
    if (!query.message) {
      return res.status(400).json({
        error: 'message required'
      });
    } else if (!query.body) {
      return res.status(400).json({
        error: 'sender email required in field "email"'
      });
    }
    msg = {
      to: 'david@whealetech.com',
      from: 'contactme@davidwheale.com',
      subject: 'Contact from davidwheale.com',
      text: query.message,
      reply_to: query.email
    };
    delete query.email;
    delete query.message;
    for (const field in query) {
      msg.text = msg.text.concat(`\n${ field }: ${ query[field] }`);
    }
  }

  sgMail.send(msg, (error, result) => {
    if (error) {
      return next(error);
    } else {
      res.status(202).send(result);
    }
  });
});

module.exports = router;