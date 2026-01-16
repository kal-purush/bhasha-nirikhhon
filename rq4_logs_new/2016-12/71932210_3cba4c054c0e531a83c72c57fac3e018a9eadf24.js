'use strict';

var CarsService = require('../services/cars');
var jwt = require('jwt-simple');
var secrets = require('../config/secrets');

class CarsController {
    constructor(router, passport) {
        this.router = router;
        this.passport = passport;
        this.registerRoutes();
    }

    registerRoutes() {
        // this.router.get('/car', this.passport.authenticate('jwt', { session: false }), this.getCar.bind(this));
        this.router.post('/car', this.passport.authenticate('jwt', { session: false }), this.postCar.bind(this));
    }

    getCar(req, res) {
        var carInfo = req.body
        CarsService.getOne(carInfo.plate, function (resp) {
            if (resp.status == "success") {
                res.status(200).send(resp);
            }
            else {
                res.status(500).send(resp)
            }
        });
    }

    postCar(req, res) {
        var carInfo = req.body.data;
        CarsService.addCar(carInfo, function (resp) {
            if (resp.status == "success") {
                res.status(200).send(resp);
            }
            else {
                res.status(500).send(resp)
            }
        });
    }

}

module.exports = CarsController;