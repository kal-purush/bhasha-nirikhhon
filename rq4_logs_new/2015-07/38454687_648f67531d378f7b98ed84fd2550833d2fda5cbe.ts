/// <reference path="../../typings/node/node.d.ts"/>
/// <reference path="../../typings/express/express.d.ts"/>

import express = require('express');
var userService = require('../services/userStore.js');

//module UserController {

    export function getUsers(req : express.Request, res : express.Response) {
        userService.getUsers(function(err, users) {
            res.format({
                'application/json': function(){
                    res.json(users);
                }
            });
        });
    }

    export function registerUser (req : express.Request, res : express.Response) {
        userService.registerUser(req.body.email, req.body.password, function(err, user) {
            res.format({
                'application/json': function(){
                    res.json(user);
                },
            });
        });
    }

    export function login (req : express.Request, res : express.Response) {

    }
//}