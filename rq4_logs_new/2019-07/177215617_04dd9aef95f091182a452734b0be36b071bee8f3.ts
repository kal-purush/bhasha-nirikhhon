import { Request, Response, NextFunction } from "express";
import { statModel } from "../../models/stat";
import axios = require('axios');

export class WebAppStatController {

    public async renderViewAllStatsPage(req: Request, res: Response, next: NextFunction) {
        console.log("Render All Stats Requested.");
        const buffs = await statModel.find().exec();
    }
}