var router = require('express').Router();
const { requiresAuth } = require('express-openid-connect');

router.get('/', function (req, res, next) {
  res.render('index', {
    title: 'Competiton maker',
    isAuthenticated: req.oidc.isAuthenticated()
  });
});

router.get('/competitions', requiresAuth(), function (req, res, next) {
  res.render('competitions', {
    userProfile: JSON.stringify(req.oidc.user, null, 2),
    title: 'Competitions'
  });
});

router.get('/competition/:link', function (req, res, next) {
  const link = req.params.link;
  res.locals.userProfile = JSON.stringify(req.oidc.user, null, 2);

  res.render('competition', {
    title: 'Competition',
    link: link
  });
});

module.exports = router;