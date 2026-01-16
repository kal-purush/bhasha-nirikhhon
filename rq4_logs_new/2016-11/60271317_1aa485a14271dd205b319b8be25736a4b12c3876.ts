import * as eta from "eta-lib";

import * as express from "express";

export class Model implements eta.Model {
    public render(req: express.Request, res: express.Response, callback: (env: { [key: string]: any }) => void): void {
        eta.student.get(req.session["userid"], (student: eta.Student) => {
            if (!student) {
                callback({ errcode: eta.http.InternalError });
                return;
            }

            if (student.sections.length > 0) {
                if (eta.section.hasCurrentSections(student.sections, eta.term.getCurrent().id)) {
                    res.redirect("/track/student");
                    return;
                }
            }
            // falls to here if they're not a current student
            eta.professor.getSections(req.session["userid"], (sections: eta.Section[]) => {
                if (!sections) {
                    callback({ errcode: eta.http.InternalError });
                    return;
                }
                if (sections.length > 0) {
                    if (eta.section.hasCurrentSections(sections, eta.term.getCurrent().id)) {
                        res.redirect("/track/professor");
                        return;
                    }
                }
                // falls to here if they're not a current student or professor
                eta.athlete.isDirector(req.session["userid"], (isAthleteDirector: boolean) => {
                    if (isAthleteDirector === null) {
                        callback({ errcode: eta.http.InternalError });
                        return;
                    }
                    if (isAthleteDirector) {
                        res.redirect("/track/athlete");
                        return;
                    }
                    // falls to here if they're not a current student, professor or director
                    callback({}); // just display the "error" page
                });
            });
        });
    }
}