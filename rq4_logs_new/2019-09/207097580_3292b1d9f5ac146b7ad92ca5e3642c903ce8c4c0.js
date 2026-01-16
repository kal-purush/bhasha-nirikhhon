const env = require('../environment');
const nodemailer = require("nodemailer");

const transporter = nodemailer.createTransport({
    host: env.noReplyMail.outgoingServerSMTP.host,
    port: env.noReplyMail.outgoingServerSMTP.sslPort,
    secure: true,
    auth: {
        user: env.noReplyMail.userName,
        pass: env.noReplyMail.password
    }
});

function triggerMail(mailId) {

    const mailOptions = {
        from: `Mail Name <${env.noReplyMail.userName}>`,
        // from: env.noReplyMail.userName,
        to: mailId,
        subject: subject,
        text: text,
    };

    transporter.sendMail(mailOptions, (err, info) => {
        if (err) throw err
        else console.log('success')
    });
}