import { Request, Response, NextFunction } from "express";
import axios = require('axios');

export class WebAppBuffController {

    public async deleteBuff(req: Request, res: Response, next: NextFunction) {
        console.log("Received Buff delete request: " + req.url);
        try {
            await axios.default.delete("/rest/buffs?_id=" + req.query._id, { baseURL: "http://localhost:3000" });
            res.redirect('/buffs');
        } catch (error) {
            console.log(error);
            return next(error);
        }
    }
}