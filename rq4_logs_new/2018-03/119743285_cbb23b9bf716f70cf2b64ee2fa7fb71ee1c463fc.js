var express = require('express');
var router = express.Router();

var jwt = require('jsonwebtoken');

var router = express.Router();

var game_ip = "192.168.0.10";

router.get('/api/gameserver', function(req, res) {
  res.json({
    ip: game_ip
  });
});

router.post('/api/gameserver', function(req, res) {

  var ip = req.body.ip;
  game_ip = ip;

  res.json({
    success: true
  })

});

module.exports = router;