import { inject, injectable } from "inversify";
import { controller, httpGet, httpPost, interfaces, requestParam } from "inversify-express-utils";
import { DatabaseService } from "../../core/services/database.service";
import {Request, Response } from "express";
import { LoggerService } from "../../core/services/logger.service";


@controller('/v1/login')
@injectable()
export class LoginController implements interfaces.Controller{
    constructor(@inject(DatabaseService.name) private databaseService: DatabaseService, @inject(LoggerService.name) private loggerService: LoggerService) {

    }

    @httpGet('/user/:username&:password')
    public login(request: Request, response: Response): void{
        this.databaseService.loginUser(request.params.username, request.params.password).then((result) =>{
            response.status(200).json(result);
        })
    }
}