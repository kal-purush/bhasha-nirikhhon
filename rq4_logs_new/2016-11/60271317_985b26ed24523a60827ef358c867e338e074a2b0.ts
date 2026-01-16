import * as eta from "eta-lib";

import * as express from "express";
import * as nodemailer from "nodemailer";
import * as querystring from "querystring";

export class Model implements eta.Model {
    public render(req: express.Request, res: express.Response, callback: (env: { [key: string]: any }) => void): void {
        if (!eta.params.test(req.body, ["name", "email", "phone", "message"])) {
            callback({ errcode: eta.http.InvalidParameters });
            return;
        }
        eta.mail.sendMail({
            "from": req.body.email,
            "to": "mathmac@iupui.edu",
            "subject": "Comments submitted from MAC website",
            "text": "From: " + req.body.name + "\nEmail: " + req.body.email
            + "\nPhone: " + req.body.phone + "\nMessage:\n" + req.body.message
        }, (err: Error, info: nodemailer.SentMessageInfo) => {
            if (err) {
                eta.logger.warn("Could not send mail from " + req.body.email + ": " + err.message);
                res.redirect("/contact?error=" + querystring.escape("Failed to send email."));
                return;
            }
            res.redirect("/contact?success=true");
        });
    }
}