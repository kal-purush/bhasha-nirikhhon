import nodemailer from "nodemailer";

export interface IMailer {
  send(to: string, subject: string, text: string, html: string): Promise<void>;
}

export default class Mailer implements IMailer {
  private transporter = nodemailer.createTransport({
    host: process.env.EMAIL_HOST,
    port: Number(process.env.EMAIL_PORT),
    secure: false,
    auth: {
      user: process.env.EMAIL_USER,
      pass: process.env.EMAIL_PASSWORD,
    },
  });

  async send(to: string, subject: string, text: string, html: string): Promise<void> {
    await this.transporter.sendMail({
      from: `"Shopping List" <${process.env.EMAIL_USER}>`,
      to,
      subject,
      text,
      html,
    });
  }
}