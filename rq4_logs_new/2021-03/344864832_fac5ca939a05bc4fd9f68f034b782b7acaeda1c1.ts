
import express from 'express';
import { STATE, UserModel } from '../models/User';
import { userService } from '../services/userService';

export const active = async (req: express.Request, res: express.Response, next: express.NextFunction) => {
    let searchQuery = null;
    
    if (req.headers.id) {
        searchQuery = { _id: req.headers.id as string }
    } else if (req.body.email) {
        searchQuery = { email: req.body.email }
    }

    if (!searchQuery) {
        return res.status(401).send({
            type: 'ERROR',
            message: 'User not authorised'
        });
    }

    const user = await userService.findUserByQuery(searchQuery);
    if (!user) {
        return res.status(404).send({
            type: 'ERROR',
            message: 'User not found'
        });
    }

    if (user.state !== STATE.ACTIVE) {
        return res.status(401).send({
            type: 'ERROR',
            message: 'User is deleted or blocked'
        });
    }

    next();
}