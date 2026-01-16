import {Request, Response, NextFunction} from 'express';
import {User} from '../../models/User';
import {Md5} from 'ts-md5';

export let controller = {
    get: (req: Request, res: Response, next: NextFunction) => {
        let id = req.user.id;
        User.scope('full').findById(id).then(user => {
            if (user == null) {
            res.status(404).json({
                message: 'User Not Found'
            });
            } else {
                res.json(user);
            }
        }).catch(error => {
            res.status(500).json({
                message: 'Unknown error has occured'
            });
        });
    },
    put: (req: Request, res: Response, next: NextFunction) => {
        let { name, dateOfBirth, country, region, postalCode, phoneNumber} = req.body;
        let id = req.user.id;
        User.scope('full').findById(id).then(user => {
            if (user == null) {
                res.status(404).json({
                    message: 'User not found'
                });
            } else {
                user.name = name;
                user.dateOfBirth = dateOfBirth;
                user.country = country;
                user.region = region;
                user.postalCode = postalCode;
                user.phoneNumber = phoneNumber;
                user.save().then(savedUser => {
                    res.json(savedUser);
                }).catch(error => {
                    res.status(400).json({
                        message: 'Failed to save user'
                    });
                });
            }
        }).catch(error => {
            res.status(500).json({
                message: 'Unknown error has occured'
            });
        });
    }
};