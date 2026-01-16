import * as eta from "eta-lib";

import * as fs from "fs";

import * as express from "express";

export class Model implements eta.Model {
    public render(req: express.Request, res: express.Response, callback: (env: { [key: string]: any }) => void): void {
        let dir: string = eta.server.modules["office"].baseDir + "static/files/employee-files/";
        fs.mkdir(dir + req.body.id, function(err: any) {
            if (!err || err.code == 'EEXIST') {
                fs.writeFileSync(dir  + req.body.id + "/"+ (<any>req.files)[0].originalname, (<any>req.files)[0].buffer);
                callback({});
            }
            else {
                callback(err);
            }
        });
    }
}