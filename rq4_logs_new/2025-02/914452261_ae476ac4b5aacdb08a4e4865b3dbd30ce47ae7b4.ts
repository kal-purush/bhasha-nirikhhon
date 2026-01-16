import { env } from 'node:process';
import nodemailer from 'nodemailer';
import { ContactInputDTO } from "src/infrastructure/api/contact/contact.dto";

const transporter = nodemailer.createTransport({
  host: env['MAIL_HOST'],
  port: Number(env['MAIL_PORT']),
  secure: false,
  auth: {
    user: env['MAIL_USER'],
    pass: env['MAIL_PASS'],
  },
})

export async function sentEmail(data: ContactInputDTO) {
  const { name, email, message } = data;

  return await transporter.sendMail({
    from: email,
    to: env['MAIL_TO'],
    subject: `[CV] - Message from ${name}`,
    text: message,
  })
}