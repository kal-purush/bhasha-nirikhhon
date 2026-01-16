import { SmtpAdapter } from "@/shared/adapters";
import { createTransport } from "nodemailer";

export class NodemailerAdapter implements SmtpAdapter {
  private client = createTransport({
    host: "smtp.mailtrap.io",
    port: 2525,
    auth: {
      user: "59af6c07ee1d8d",
      pass: "54e6847d3fe895",
    },
  });

  public async send(input: SmtpAdapter.Input): Promise<void> {
    const message = {
      from: "test",
      to: input.email,
      subject: `here you link confirmation http://127.0.0.1:333/user_confirmation/${input.id}`,
    };

    await this.client.sendMail(message);
  }
}