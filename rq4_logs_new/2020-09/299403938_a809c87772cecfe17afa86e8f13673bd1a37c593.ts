import { createTransport } from "nodemailer";
import { BASE_URL_EMAIL_VERIFICATION } from "./constants";
import { verifyMailTemplate } from "./templates/verify-mail";
export async function sendMail(email_address: string, emailBody: any) {
  let transporter = createTransport({
    host: "smtp.gmail.com",
    port: 587,
    secure: false,
    auth: {
      user: process.env.EMAIL_USERNAME || "",
      pass: process.env.EMAIL_PASSWORD || "",
    },
  });

  let info = await transporter.sendMail({
    from: "Desk",
    to: email_address, // list of receivers must be string of emails seperated by commas
    cc: "harshgoel05@gmail.com",
    subject: emailBody.subject,
    text: emailBody.text,
    html: emailBody.html,
  });

  console.log("Message sent: %s", info.messageId);
}

export function verifyYourEmail(email: any) {
  return {
    subject: "[IMPORTANT] Confirm your email",
    text: "Please verify your email",
    html: verifyMailTemplate(BASE_URL_EMAIL_VERIFICATION + email),
  };
}