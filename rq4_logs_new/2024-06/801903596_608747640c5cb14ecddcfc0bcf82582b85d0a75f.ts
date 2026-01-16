import nodemailer from 'nodemailer';
import ejs from 'ejs';
import fs from 'fs';
import path from 'path';

export class NodeMailerService {
  private readonly transporter;
  private readonly sendOptions;
  constructor() {
    this.transporter = nodemailer.createTransport({
      service: 'gmail',
      auth: {
        user: process.env.NODEMAILER_USER,
        pass: process.env.NODEMAILER_PASSWORD,
      },
    });
    this.sendOptions = {
      from: process.env.NODEMAILER_USER,
    };
  }

  private async renderTemplate(
    templateName: string, 
    data: any,
  ): Promise<string> {
    const filePath = 
    path.join(__dirname, '../email-templates', `${templateName}.ejs`);
    const template = fs.readFileSync(filePath, 'utf-8');
    return ejs.render(template, data);
  }


  public async sendEmail(
    to: string, 
    subject: string, 
    templateName: string, 
    data: any,
  ) {
    try{
      const html = await this.renderTemplate(templateName, data);
      await this.transporter.sendMail({
        ... this.sendOptions,
        to: to,
        subject: subject,
        html: html,
      });
    } catch (error) {
      console.error('Error sending email: ', error);
    }
  }
}