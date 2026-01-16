/// <reference path="../../typings/index.d.ts"/>
// For "marked" typings

import * as eta from "eta-lib";

import * as express from "express";

import * as marked from "marked";

export class Model implements eta.Model {

    public render(req: express.Request, res: express.Response, callback: (env: { [key: string]: any }) => void): void {

        let sql : string = `SELECT * FROM Policy ORDER BY category, name`;
        eta.db.query(sql, [], (err: eta.DBError, rows: any[]) => {
            if (err) {
                eta.logger.error(err);
                return callback({
                    errcode: eta.http.InternalError
                });
            }
            for (let i: number = 0; i < rows.length; i++) {
                rows[i].body = marked(rows[i].body);
            }
            callback({
                policies: rows
            });
        });
    }
}