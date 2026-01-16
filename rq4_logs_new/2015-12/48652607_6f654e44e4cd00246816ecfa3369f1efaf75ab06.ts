/// <reference path="../../../../typings/tsd.d.ts" />

'use strict';

import * as express from 'express';
import * as jwt from 'express-jwt';
import * as cors from 'cors';
import * as secret from '../../auth/secret';
import * as jsonfileservice from '../../utils/jsonfileservice';
import * as four0four from '../../utils/404';
import * as _ from 'underscore';
import * as tokenManager from '../../auth/tokenManager';
import * as userManager from '../../auth/userManager';
import * as _user from '../../models/user';
var User = _user.User;

export = (api?: string): express.Router => {

    api = api || '/api/v1/';
    var authorized: jwt.Options = {
        secret: secret.secretToken
    };
    var anonymous: jwt.Options = {
        secret: secret.secretToken,
        credentialsRequired: false
    };
    var data = '/../../../data/';
    var expiresIn = '1d';

    var router = express.Router();

    router.options('*', cors());

    // routes
    router.post(api + 'authenticate', authenticate);
    router.post(api + 'token', jwt(authorized), token);

    // send the refreshed token for below endpoints
    router.use(jwt(anonymous), tokenRefresh);

    router.get(api + 'getuserinfo', jwt(authorized), getUserInfo);
    router.get(api + 'customer/:id', jwt(authorized), getCustomer);
    router.get(api + 'customers', jwt(authorized), getCustomers);
    router.get(api + 'ping', (req, res) => {
        res.send('pong');
    });

    function authenticate(req: express.Request, res: express.Response) {

        var username = req.headers['username'];
        var password = req.headers['password'];

        if (!username || !username) {
            return res.sendStatus(400);
        }

        userManager.authenticate(username, password, (user) => {
            if (!user) {
                return res.send(401);
            }

            var token = tokenManager.create(user, expiresIn);
            res.json(token);
        });
    }

    function token(req: express.Request, res: express.Response) {
        var token = tokenManager.create(req.user, expiresIn);
        res.json(token);
    }

    function getUserInfo(req: express.Request, res: express.Response) {
        var user = User.findById(req.user.id, (err, user) => {
            var clientUser = _.omit(user.toObject(), ['_id', 'password', '__v']);
            return res.json(clientUser);
        });
    }

    function getCustomer(req: express.Request, res: express.Response) {
        var id = req.params.id;
        var msg = 'customer id ' + id + ' not found. ';
        try {
            var json = jsonfileservice.getJsonFromFile(data + 'customers.json');
            var customer = json.filter((c) => {
                return c.id === parseInt(id);
            });
            if (customer && customer[0]) {
                res.send(customer[0]);
            } else {
                four0four.send404(req, res, msg);
            }
        }
        catch (ex) {
            four0four.send404(req, res, msg + ex.message);
        }
    }

    function getCustomers(req: express.Request, res: express.Response) {
        console.log(req.user);
        var msg = 'customers not found. ';
        try {
            var json = jsonfileservice.getJsonFromFile(data + 'customers.json');
            if (json) {
                res.send(json);
            } else {
                four0four.send404(req, res, msg);
            }
        }
        catch (ex) {
            four0four.send404(req, res, msg + ex.message);
        }
    }

    function tokenRefresh(req: express.Request, res: express.Response, next: Function) {
        if (req.user) {
            var token = tokenManager.create(req.user, expiresIn);
            res.setHeader('X-Access-Token', token);
        }
        next();
    }

    return router;
}