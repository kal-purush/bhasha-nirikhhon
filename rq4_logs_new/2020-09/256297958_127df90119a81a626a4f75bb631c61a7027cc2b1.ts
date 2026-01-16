import { Request, Response } from 'express';
import httpStatus from 'http-status';
import { Nullable } from '../../../../Shared/domain/Nullable';
import { MapperErrorToHttpCode } from '../../../../Shared/infrastructure/MapperErrorToHttpCode';
import { Controller } from '../../../Shared/infrastructure/controllers/Controller';
import { UserFinder } from '../../application/UserFinder';

export class UserGetIndexController implements Controller {
    constructor(private clientFinder: UserFinder) {
    }

    //TODO Terminar
    async run(req: Request, res: Response) {
        try {
            const userPetition = (<any>req).user;

            const users: Nullable<any[]> = await this.clientFinder.run(null, userPetition);
            if (users) {
                res.status(httpStatus.OK).json(users);
            } else {
                res.status(httpStatus.NOT_FOUND);
            }
        } catch (error) {
            res.status(MapperErrorToHttpCode(error));
        }
    }
}