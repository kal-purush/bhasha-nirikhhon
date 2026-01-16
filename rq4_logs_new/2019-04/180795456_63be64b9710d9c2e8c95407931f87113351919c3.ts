import express from 'express';
var router = express.Router();

var headlineController = require('../../controllers/headline.controller');

router.get('/headlines', headlineController.getHeadlines);

module.exports = router;