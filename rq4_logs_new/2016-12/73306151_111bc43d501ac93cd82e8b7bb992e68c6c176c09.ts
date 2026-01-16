'use strict';

import express = require('express');
export let authenticationRoute = express.Router();
import {IUserModel, User} from '../models/user.model'
let createError = require('http-errors');

authenticationRoute.post('/api/authenticate', function (req: express.Request, res: express.Response, next: express.NextFunction) {
    let jsonBody: string = JSON.stringify(req.body);
    console.log(`authenticate: ${jsonBody}`);
    let username: String = req.body.username;
    let password: String = req.body.password;
    let selector = {'username': username}
    User.find(selector, (err, users) => {
        if (err) {
            res.status(401).json({error: `username ${username} unknown. ${err}`});
        } else {
            // verify the password
            if (users.length && users[0].password === password) {
                res.json({token: 'fake-jwt-token ' + users[0].id});
            } else {
                res.status(401).json({error: `User ${username} wrong password`});
            }
        }
    });
});

authenticationRoute.use('/api', function (req: express.Request, res: express.Response, next: express.NextFunction) {
    let authHeader = req.get('Authorization');
    if (authHeader && authHeader.startsWith('Bearer fake-jwt-token ')) {
        let userIdFromFakeToken = authHeader.split(' ')[2];
        // TODO: test token and return 401 if invalid else do nothing!
        console.log('user is authenticated');
        next();
    } else {
        res.status(401).json({error: 'not yet authenticated'});
    }
});
