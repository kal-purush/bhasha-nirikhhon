/// <reference path="../../../typings/tsd.d.ts" />

'use strict';

import * as bcrypt from 'bcrypt';
import * as mongoose from 'mongoose';
import * as _ from 'underscore';
import * as userModel from '../models/user';
import * as roleModel from '../models/role';
import * as taskCounter from '../utils/taskCounter';

var User = userModel.User;
var Role = roleModel.Role;
var TaskCounter = taskCounter.TaskCounter;

var mongodbURL = 'mongodb://localhost/accounts';
var mongodbOptions = {};

export function init(cb: any) {

    mongoose.connect(mongodbURL, mongodbOptions, (err) => {
        if (err) {
            console.log('Connection refused to ' + mongodbURL);
            console.log(err);
            return cb(err);
        }
        else {
            console.log('Connection successful to: ' + mongodbURL);
        }
        
        // initialize db
        createRoles((err) => {
            if (err) {
                console.log('Creating roles failed');
                console.log(err);
                return cb(err);
            }

            createAdmin((err) => {
                if (err) {
                    console.log('Creating admin failed');
                    console.log(err);
                }
                return cb(err);
            })
        });
    });
}

function createAdmin(cb) {
    if (User.findOne({ username: 'admin' }, (err, res) => {
        if (err) {
            cb(err);
        }
        var user = res || new User();
        user.username = 'admin';
        user.password = '#,K:s#n$j$s6Rm';
        user.email = 'john.doe@foo.com';
        user.firstName = 'John';
        user.lastName = 'Doe';
        user.created = new Date();
        user.isAdmin = true;
        if (!user.isInRole('admin')) {
            user.roles.push('admin');
        }
        user.save((err, res) => {
            cb(err);
        });
    }));
}

function createRoles(cb) {
    var tc = new TaskCounter(cb);
    roleModel.roleNames.forEach((val) => {
        tc.inc();
        Role.findOne({ name: val }, (err, res) => {
            tc.dec(err);
            if (!res) {
                var role = new Role();
                role.name = val;
                tc.inc();
                role.save((err) => {
                    tc.dec(err);
                });
            }
        });
    });
}