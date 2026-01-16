import * as eta from "eta-lib";

import * as express from "express";

export class Model implements eta.Model {
    public render(req: express.Request, res: express.Response, callback: (env: { [key: string]: any }) => void): void {
        let sql: string = `
            SELECT
                OnlineRoom.letter,
                OnlineRoom.url,
                GROUP_CONCAT(CONCAT(Course.subject, ' ', Course.number) ORDER BY Course.subject, Course.number SEPARATOR '\\n') AS rawCourses
            FROM
                OnlineRoom
                    LEFT JOIN Course ON
                        OnlineRoom.letter = Course.room
            WHERE
                OnlineRoom.date = CURDATE()
            GROUP BY OnlineRoom.letter
            ORDER BY OnlineRoom.letter ASC`;
        eta.db.query(sql, [], (err: eta.DBError, rows: any[]) => {
            if (err) {
                eta.logger.dbError(err);
                callback({ errcode: eta.http.InternalError });
                return;
            }
            for (let i: number = 0; i < rows.length; i++) {
                if (rows[i].rawCourses) {
                    rows[i].courses = rows[i].rawCourses.split("\n");
                } else {
                    rows[i].courses = "";
                }
            }
            callback({
                "rooms": rows
            });
        });
    }
}