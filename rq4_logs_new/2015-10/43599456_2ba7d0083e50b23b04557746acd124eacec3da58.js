'use strict';

var express = require('express'),
    router = express.Router(),
    bodyParser = require('body-parser'), //parses information from POST
    methodOverride = require('method-override'); //used to manipulate POST

router.use(bodyParser.urlencoded({ extended: true }));
router.use(methodOverride(function(req, res) {
    var method;
    if (req.body && typeof req.body === 'object' && '_method' in req.body) {
        // look in urlencoded POST bodies and delete it
        method = req.body._method;
        delete req.body._method;
        return method;
    }
}));

router.route('/')
    // GET
    .get(function(req, res, next) {

        res.format({
            json: function() {
                res.json({
                    pong: true,
                    ip: req.ip,
                    time: Math.floor(Date.now() / 1000)
                });
            }
        });
    });

module.exports = router;