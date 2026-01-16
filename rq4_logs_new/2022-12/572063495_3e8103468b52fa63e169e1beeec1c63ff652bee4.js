const argon2 = require('argon2');
const express = require('express');

const router = express.Router();

const ants = require('../../../models/ants');
const cookies = require('../../../models/cookies');
const users = require('../../../models/users');

router.post('/', async (req, res) => {
  console.log(req.cookies)
  if (!req.cookies?.['antlab-session'])
    return res.status(401).json({
      status: 'error',
      message: 'Unauthorized',
    });

  const cookie = await cookies.findOne({
    cookie: req.cookies['antlab-session']
  }).populate('user').exec();

  if (!cookie)
    return res.status(401).json({
      status: 'error',
      message: 'Unauthorized'
    });

  const user = cookie.user;

  if (await argon2.verify(user.password, req.body.oldPassword)) {
    user.password = await argon2.hash(req.body.newPassword);

    await user.save();

    return res.json({
      status: 'success',
      message: 'Password changed'
    });
  } else {
    return res.status(401).json({
      status: 'error',
      message: 'Unauthorized'
    });
  }
});

module.exports = router;