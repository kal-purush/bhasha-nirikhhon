import { Request, Response } from 'express';
import * as Http from '../../util/http';
import * as jwt from 'jsonwebtoken';
import { Md5 } from 'md5-typescript';
import User from '../../models/user';
import { validationResult } from 'express-validator/check';


const signup = (req: Request, res: Response) => {
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
        return Http.BadRequestResponse(res, { errors: errors.array() });
    }
    const user = new User({
        username: req.body.username,
        email: req.body.email,
        password: Md5.init(req.body.password)
    });
    user.save()
        .then((rs: any) => {
            if (!rs) {
                return Http.InternalServerResponse(res);
            }
            // create a token
            const token = jwt.sign({ id: user.id }, 'secret', {
                expiresIn: 86400 // expires in 24 hours
            });
            return Http.SuccessResponse(res, {
                token: token
            });
        })
        .catch((e: any) => {
            console.error(e);
            return Http.InternalServerResponse(res);
        });
};

const signin = (req: Request, res: Response) => {
    return res.render('index', { title: 'Welcome to cars club.' });
};

export { signup, signin };