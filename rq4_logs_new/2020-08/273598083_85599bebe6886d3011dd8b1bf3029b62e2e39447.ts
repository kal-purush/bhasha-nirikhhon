import * as nodemailer from "nodemailer";
import config from '../configs/config';

class Mail {

    constructor(
        public to?: string,
        public subject?: string,
        public message?: string) { }


    sendMail() :any{

        let mailOptions = {
            from: config.user,
            to: this.to,
            subject: this.subject,
            html: this.message
        };

        const transporter = nodemailer.createTransport({
            service:'gmail',
            auth: {
                user: config.user,
                pass: config.password
            }
        });

        transporter.sendMail(mailOptions, function (error, info) {
            if (error) {                
                return error;
            } else {
                
                return info.response;
            }
        });
    }


}

export default new Mail;