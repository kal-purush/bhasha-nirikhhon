// lib/sendEmail.ts
import { Resend } from "resend";


const resend = new Resend(process.env.RESEND_API_KEY);

export async function sendResetEmail(to: string, resetLink: string) {
  try {
    const { data, error } = await resend.emails.send({
      from: "onboarding@resend.dev",
      to,
      subject: "🔁 Reset your password",
      html: `
  <div style="font-family: sans-serif; color: #333; max-width: 600px; margin: auto;">
    <h2 style="color: #2c3e50;">🔁 Reset Your Password</h2>
    <p>Hi there,</p>
    <p>You requested a password reset for your account.</p>
    <p>
      Click the button below to reset your password. This link is valid for 1 hour:
    </p>
    <p style="text-align: center; margin: 30px 0;">
      <a href="${resetLink}" style="
        background-color: #0070f3;
        color: white;
        padding: 12px 24px;
        border-radius: 8px;
        text-decoration: none;
        font-weight: bold;
        display: inline-block;
      ">Reset Password</a>
    </p>
    <p>If you did not request this, you can safely ignore this email.</p>
    <hr />
    <p style="font-size: 12px; color: #888;">© ${new Date().getFullYear()} My Healthcare System</p>
  </div>
`,
    });

    if (error) {
      console.error("Resend error:", error);
      return false;
    }

    return true;
  } catch (err) {
    console.error("Email sending failed:", err);
    return false;
  }
}