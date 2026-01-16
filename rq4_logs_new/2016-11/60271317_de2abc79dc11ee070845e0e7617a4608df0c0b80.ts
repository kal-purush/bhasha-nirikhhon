import * as eta from "eta-lib";

import * as express from "express";

export class Model implements eta.Model {
    public render(req: express.Request, res: express.Response, callback: (env: { [key: string]: any }) => void): void {
        eta.athlete.isDirector(req.session["userid"], (isAthleteDirector: boolean) => {
            if (isAthleteDirector === null) {
                callback({ errcode: eta.http.InternalError });
                return;
            }
            if (!isAthleteDirector) {
                res.redirect("/track/index");
                return;
            }
            let isPastWeek: boolean = !!req.query.isPastWeek;
            let pastWeekSql: string = `AND DATE(Visit.timeIn) >= (CURDATE() - INTERVAL 7 DAY)`;
            let sql: string = `
                SELECT
                    Person.id,
                    Person.firstName,
                    Person.lastName,
                    VisitCount.count AS visitCount,
                    VisitCount.duration AS visitDuration,
                    VisitCount.timesOut AS visitTimesOut,
                    VisitCount.lastTimeIn AS visitLastTimeIn
                FROM
                    Athlete
                        LEFT JOIN Person ON
                            Athlete.id = Person.id
                        LEFT JOIN (
                            SELECT
                                Visit.student AS student,
                                COUNT(DISTINCT Visit.student, Visit.timeIn) AS count,
                                ROUND(SUM(TIME_TO_SEC(TIMEDIFF(Visit.timeOut, Visit.timeIn))) / 3600, 2) AS duration,
                                GROUP_CONCAT(IFNULL(Visit.timeOut, 'NULL') ORDER BY Visit.timeIn DESC SEPARATOR ', ') AS timesOut,
                                DATE(MAX(Visit.timeIn)) = CURDATE() AS lastTimeIn
                            FROM
                                Visit
                            WHERE
                                Visit.term = ?
                                ${isPastWeek ? pastWeekSql : ""}
                            GROUP BY
                                Visit.student
                        ) VisitCount ON
                            Athlete.id = VisitCount.student
                WHERE
                    VisitCount.count > 0`;
            eta.db.query(sql, [eta.term.getCurrent().id], (err: eta.DBError, rows: any[]) => {
                if (err) {
                    eta.logger.dbError(err);
                    callback({ errcode: eta.http.InternalError });
                    return;
                }
                for (let i: number = 0; i < rows.length; i++) {
                    let lastTimeOut: string = rows[i].visitTimesOut.split(", ")[0];
                    rows[i].isCurrent = (lastTimeOut === "NULL") && rows[i].visitLastTimeIn;
                }
                callback({
                    "students": rows,
                    "isPastWeek": isPastWeek
                })
            });
        });
    }
}