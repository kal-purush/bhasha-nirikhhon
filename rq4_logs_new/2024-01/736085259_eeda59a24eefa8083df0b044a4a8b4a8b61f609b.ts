const nodemailer = require("nodemailer");

require('dotenv').config();

const transporter = nodemailer.createTransport({
    service: 'gmail',
    auth: {
        user: process.env.GMAIL_USERNAME,
        pass: process.env.GMAIL_PASSWORD,
    }
});


async function sendEmail(toUser: string, token: string){
    const info = await transporter.sendMail({
        from: `"Wok Day" <${process.env.GMAIL_USERNAME}`,
        to: toUser,
        subject: "Please verify your email",
        text: `Please verify your email \n ${token}`,
        html: `<a href='http://localhost:3000/${token}'>Click here to verify your email</a>`
    });
    console.log("Message sent: %s", info.messageId);
} 
module.exports = {
    sendEmail
}