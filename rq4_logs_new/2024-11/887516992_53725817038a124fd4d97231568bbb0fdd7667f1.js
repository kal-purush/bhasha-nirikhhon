require("dotenv").config();
const nodemailer = require("nodemailer");

// First, log the configuration (with password length only for security)
console.log("Config:", {
  email: process.env.EMAIL_USER,
  passwordLength: process.env.EMAIL_APP_PASSWORD?.length,
});

const transporter = nodemailer.createTransport({
  host: "smtp.gmail.com",
  port: 465,
  secure: true,
  auth: {
    user: process.env.EMAIL_USER,
    pass: process.env.EMAIL_APP_PASSWORD,
  },
});

async function testEmail() {
  try {
    // Verify connection configuration
    await transporter.verify();
    console.log("Verification successful, attempting to send email...");

    const info = await transporter.sendMail({
      from: process.env.EMAIL_USER,
      to: process.env.EMAIL_USER,
      subject: "Test Email",
      text: "This is a test email to verify the configuration.",
    });

    console.log("Email sent successfully!");
    console.log("Message ID:", info.messageId);
  } catch (error) {
    console.error("Error details:", {
      message: error.message,
      code: error.code,
      response: error.response,
    });
  }
}

testEmail();