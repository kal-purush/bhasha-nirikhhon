import * as nodemailer from "nodemailer";
import { NextFunction, Request, Response } from "express";
import { InvoiceGetFromClientType } from "@/types";

export async function sendTestEmail(
  req: Request,
  res: Response,
  next: NextFunction
) {
  const testAccount = await nodemailer.createTestAccount();
  const { clientEmail, clientName }: InvoiceGetFromClientType = req.body;

  const transporter = nodemailer.createTransport({
    host: "smtp.ethereal.email",
    port: 587,
    secure: false,
    auth: {
      user: testAccount.user,
      pass: testAccount.pass,
    },
  });

  const info = await transporter.sendMail({
    from: "sender@example.com",
    to: clientEmail,
    subject: "Test Email",
    text: `<h1>new invoice for ${clientName} </>`,
  });

  console.log("Test email sent: ", nodemailer.getTestMessageUrl(info));
  next();
}