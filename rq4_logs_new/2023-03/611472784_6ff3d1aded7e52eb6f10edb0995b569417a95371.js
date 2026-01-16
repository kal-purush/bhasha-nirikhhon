const nodemailer = require('nodemailer');


class emailSend {
    async makeAccount(details) {
        const account = {
            user: "gaalmark.markit@gmail.com",
            pass: process.env.EMAIL_PASS
        };


        const transporter = nodemailer.createTransport({
            host: "smtp.gmail.com",
            port: 587,
            secure: false,
            auth: {
                user: account.user,
                pass: account.pass,
            },
        });
        let info = await transporter.sendMail({
            from: details.from,
            to: "gaalmark.markit@gmail.com",
            subject: details.subject,
            text: "",
            html: details.html
        });
        return "Message sent: " + info.messageId;
    }

    async EmailSending(email) {
        let html = email.name + "<br>" + email.email + "<br>" + email.text;

        const details = {
            from: email.email,
            to: "gaalmark.markit@gmail.com",
            html: html,
            subject: email.subject
        }
        return this.makeAccount(details);
    }

}

module.exports = new emailSend();