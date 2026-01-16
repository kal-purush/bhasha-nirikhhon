/*
* @Author: x
* @Date:   2018-02-25 17:59:21
* @Last Modified by:   ChencongDiu
* @Last Modified time: 2018-02-25 18:00:04
*/
const express = require('express');
const router = express.Router();

router.get('/', (req, res, next) => {
  res.send('contact get');
});

router.post('/', (req, res, next) => {
  res.send('contact post');
});

module.exports = router;