import express from 'express';
import nodemailer from 'nodemailer';
import handlebars from 'handlebars';
import fs from 'fs';
import path from 'path';

const connection = {
    host: process.env.MAIL_HOST,
    port: Number(process.env.MAIL_PORT),
    auth: {
        user: process.env.MAIL_USER,
        pass: process.env.MAIL_PASSWORD
    }
};

const transporter = nodemailer.createTransport(connection);

export const sendEmail = async (email: string, payload: any, res: express.Response) => {
    try {
        const source = fs.readFileSync(path.join(__dirname, './templates/requestresetPassword.handlebars'), "utf8");
        const compiledTemplate = handlebars.compile(source);
        const mailOptions = {
            from: 'Blog team',
            to: email,
            subject: 'Reset password link',
            text: 'Hey there, it’s our first message sent with Nodemailer ',
            html: compiledTemplate(payload),
        };
        transporter.sendMail(mailOptions, (error, _info) => {
            if (error) {
                return error;
            } else {
                return res.status(200).json({
                    success: true,
                });
            }
        });
    } catch (error) {
        return error;
    }
}