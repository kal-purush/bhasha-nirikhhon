import { Request, Response } from "express";

export default (req: Request, res: Response) => {
    res.statusCode = 200;
    res.json({
        "message": "Hello from the Typescript/Node JS/Express based DAZN coding challenge service!"
    });
};