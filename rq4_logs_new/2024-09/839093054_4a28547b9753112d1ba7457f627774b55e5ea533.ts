'use server'

import nodemailer from 'nodemailer';
import { env } from "~/env";

const transporter = nodemailer.createTransport({
  service: 'gmail',
  host: "smtp.gmail.com",
  port: 587,
  secure: false, // true for port 465, false for other ports
  auth: {
    user: process.env.MAIL_USER,
    pass: process.env.MAIL_PASSWORD,
  },
});
const sendEmail = async () => {
  const info = await transporter.sendMail({
    from: '"Querétaro si sonrie"', // sender address
    to: "user", // list of receivers
    subject: "Recuperar contraseña", // Subject line
    text: "Da click en el siguiente link para recuperar tu contraseña: http://localhost:3000/access-page/" , // plain text body
  });

  console.log(info.messageId);
}

export default sendEmail;