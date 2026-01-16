import { Request, Response } from 'express';
import User from '../../models/user';
import * as Http from '../../util/http';
import * as config from 'config';

const search = (req: Request, res: Response) => {
    User.aggregate([
        { $match: { username: { $regex: new RegExp(`${req.query.q}.*`) } } },
        {
            $project: {
                _id: 1,
                username: 1,
                email: 1,
                avatar_url: {
                    $ifNull: ['$avatar_url', config.get('default-user-avatar')]
                }
            }
        }
    ])
        .limit(15)
        .then(d => {
            return Http.SuccessResponse(res, { users: d });
        })
        .catch(e => {
            console.error(e);
            return Http.InternalServerResponse(res);
        });
};

export { search };