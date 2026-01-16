'use strict';

module.exports = (req, res, next) => {
  const {user} = req.session;

  if (!user || user.id !== 1) {
    return res.redirect(`/login`);
  }
  return next();
};