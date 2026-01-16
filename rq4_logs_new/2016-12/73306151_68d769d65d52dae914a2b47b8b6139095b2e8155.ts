'use strict';

import express = require('express');
import {IUserModel, User} from '../models/user.model'

export function addUser(req: express.Request, res: express.Response, next: express.NextFunction) {
        let user: IUserModel = new User(req.body);
        let jsonBody: string = JSON.stringify(req.body);
        console.log(`create user: ${jsonBody}`);
        user.save((err: any, addedUser: IUserModel) => {
            if (err) {
                res.status(500).json({error: `error creating user ${user.username}. ${err}`});
            } else {
                // set the user.id to the _id provided by the db
                addedUser.id = addedUser._id;
                res.status(201).json({data: addedUser});
            }
        });
    };

export function updateUser(req: express.Request, res: express.Response, next: express.NextFunction) {
        let id = req.params.id;
        let jsonBody: string = JSON.stringify(req.body);
        console.log(`update user [${id}]: ${jsonBody}`);
        User.findById(id, (err: any, user: IUserModel) => {
            if (err) {
                res.status(404).json({error: `user ${id} not found. ${err}`});
            } else {
                // copy the properties
                user.id = req.body.id;
                user.lastname = req.body.lastname;
                user.firstname = req.body.firstname;
                user.type = req.body.type;
                user.username = req.body.username;
                user.password = req.body.password;
                // save the updated user
                user.save((err: any, updatedUser: IUserModel) => {
                    if (err) {
                        res.status(500).json({error: `error updating user ${id}. ${err}`});
                    } else {
                        res.json({data: updatedUser});
                    }
                });
            }
        });
    }

export function getAllUsers(req: express.Request, res: express.Response, next: express.NextFunction) {
        User.find((err, users) => {
            if (err) {
                res.status(404).json({error: `error retrieving users. ${err}`});
            } else {
                // set the user.id to the _id provided by the db
                users.forEach((user) => user.id = user._id);
                res.json({data: users});
            }
        });
    }

export function getUser(req: express.Request, res: express.Response, next: express.NextFunction) {
        let ref = {_id: req.params.id};
        User.findById(ref, (err, user) => {
            if (err) {
                res.status(404).json({error: `error retrieving user ${ref._id}. ${err}`});
            } else {
                // set the user.id to the _id provided by the db
                user.id = user._id;
                res.json({data: user});
            }
        });
    }

export function deleteUser(req: express.Request, res: express.Response, next: express.NextFunction) {
    let ref = {_id: req.params.id};
    User.remove(ref, (err) => {
        if (err) {
            res.status(404).json({error: `error deleting user ${ref._id}. ${err}`});
        }
        res.json({data: ref._id});
    });
}

