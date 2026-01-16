var nodemailer = require('nodemailer');
var transporter = nodemailer.createTransport({
  service: 'Mailgun',
  auth: {
    user: process.env.MAILGUN_USERNAME,
    pass: process.env.MAILGUN_PASSWORD
  }
});

/**
 * GET /news
 */
exports.newsGet = function(req, res) {
  res.render('news', {
    title: 'Guardian News API'
  });
};

/**
 * POST /news
 */
exports.newsPost = function(req, res) {
 

  if (errors) {
    req.flash('error', errors);
    return res.redirect('/news');
  }

};