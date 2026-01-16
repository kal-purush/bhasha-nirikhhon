var express = require('express');
var router = express.Router();

var tasks = require('./tasks');
var contacts = require('./contacts');

router.all(/tasks?/, tasks);
router.all(/contact?s/, contacts);

module.exports = router;