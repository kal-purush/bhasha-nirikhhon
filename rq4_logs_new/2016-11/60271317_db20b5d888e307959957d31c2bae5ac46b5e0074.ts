import * as eta from "eta-lib";

import * as express from "express";

export class Model implements eta.Model {
    public render(req: express.Request, res: express.Response, callback: (env: { [key: string]: any }) => void): void {
        eta.section.getByProfessor(req.session["userid"], (rawSections: eta.Section[]) => {
            if (!rawSections) {
                callback({ errcode: eta.http.InternalError });
                return;
            }
            let sections: eta.Section[] = eta.section.removePrevious(rawSections, eta.term.getCurrent().id);
            if (sections.length === 0) { // no sections at all or no current sections
                res.redirect("/track/index");
                return;
            }
            sections.sort(eta.section.sort);
            eta.person.getByID(req.session["userid"], (person: eta.Person) => {
                if (!person) {
                    callback({ errcode: eta.http.InternalError });
                    return;
                }
                callback({
                    "name": person.firstName + " " + person.lastName,
                    "sections": sections
                });
            });
        });
    }
}