import nodemailer from "nodemailer";

interface EmailOptions {
  to: string;
  subject: string;
  text?: string;
  html?: string;
}

class EmailService {
  private transporter: nodemailer.Transporter;

  constructor() {
    this.transporter = nodemailer.createTransport({
      host: process.env.SMTP_HOST || "smtp.gmail.com",
      port: Number(process.env.SMTP_PORT) || 587,
      secure: false,
      auth: {
        user: process.env.SMTP_USER,
        pass: process.env.SMTP_PASS,
      },
    });
  }

  async sendEmail({ to, subject, text, html }: EmailOptions): Promise<void> {
    try {
      await this.transporter.sendMail({
        from: process.env.SMTP_FROM || "your-email@domain.com",
        to,
        subject,
        text,
        html,
      });
    } catch (error) {
      console.error("Error sending email:", error);
      throw new Error("Failed to send email");
    }
  }

  async sendTicketConfirmation(email: string, ticketId: string): Promise<void> {
    const subject = `Ticket Confirmation #${ticketId}`;
    const html = `
      <h1>Ticket Confirmation</h1>
      <p>Your ticket #${ticketId} has been created successfully.</p>
      <p>We will process your request as soon as possible.</p>
    `;

    await this.sendEmail({ to: email, subject, html });
  }
}

const emailService = new EmailService();
export default emailService;