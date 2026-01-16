var express = require('express');
var router = express.Router();
var Helper = require('../lib/Helpers.js');
var passport = require('passport');

/* GET io list. */
router.get('/', function(req, res, next) {
	Helper.listIOs(req, function(result){
		res.status(result.status).send(result.data);
	});
});

router.post('/', function(req, res, next) {
	Helper.addIO(req, function(result){
		req.login(result.user, function(err){});
		res.status(result.status).send(result.data);
	});
});

router.post('/:io_id', function(req, res, next) {

	console.log("TRIGGER THE IO");

	res.status(200).send({});
});

module.exports = router;