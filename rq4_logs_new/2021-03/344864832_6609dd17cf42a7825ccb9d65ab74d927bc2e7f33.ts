
import express from 'express';
import redisClient from '../dbs/redisDb';
import bcrypt from 'bcrypt';
import jwt from 'jsonwebtoken';
import { userService } from '../services/userService';
import { saltService } from '../services/saltService';

const generateToken = (email: string) => {
    const secret = require('crypto').randomBytes(64).toString('hex');
    return jwt.sign({email}, secret, { expiresIn: '2h' });
};

class UserController {
    async signup(req: express.Request, res: express.Response) {
        const passwordRegexp = new RegExp(process.env.PWD_REGEXP as string);
        const password = req.body.password;
        const email = req.body.email;
               
        if(!password || !passwordRegexp.test(password)){
            return res.status(400).send({
                type: 'ERROR',
                message: 'Password is too weak'
            });
        }

        const user = await userService.findUserByQuery({email});

        if (user) {
            return res.status(409).send({
                type: 'ERROR',
                message: 'User already exists'
            });
        }
        
        const salt = await bcrypt.genSalt(10);
        const encryptedPassword = await bcrypt.hash(password, salt);
        try {
            const newUser = await userService.createUser({
                ...req.body,
                password: encryptedPassword
            });
            await saltService.createSaltForUser(newUser._id, salt);
            const token = generateToken(email);
            redisClient.set(newUser._id, token);
            res.json({
                ...req.body,
                token,
                password: ''
            });
            redisClient.set('test', 'token');
        } catch (e) {
            console.log(e)
            return res.status(500).send({
                type: 'ERROR',
                message: 'Error occurs creating user'
            });
        }
    }
}

const userController = new UserController();

export default userController;