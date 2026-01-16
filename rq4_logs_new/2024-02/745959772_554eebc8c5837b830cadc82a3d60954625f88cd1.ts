// POST req to submit email
import { NextRequest, NextResponse } from "next/server";
import nodemailer from "nodemailer";

export async function POST(req: NextRequest) {
  try {
    const { name, email, phone, message } = await req.json();

    const {
      EMAIL_HOST,
      EMAIL_PORT,
      USER_EMAIL,
      USER_EMAIL_PASS,
      USER_EMAIL_TO,
    } = process.env;

    if (!EMAIL_HOST && !USER_EMAIL_TO) return;

    const transporter = nodemailer.createTransport({
      //Para produção, use variáveis de ambiente para as credenciais
      service: "Outlook365",
      // host: EMAIL_HOST,
      port: parseInt(EMAIL_PORT!),
      secure: false,
      auth: {
        user: USER_EMAIL,
        pass: USER_EMAIL_PASS,
      },
      // ignoreTLS: true,
      tls: {
        ciphers: "SSLv3",
      },
    });

    // Opções do e-mail
    const mailOptions = {
      from: USER_EMAIL,
      to: USER_EMAIL_TO,
      subject: "Contato - QuinteraTI",
      html: `
        <h3>De: ${email}</h3>
        <p>${message}<p/>
        <footer>
          Atenciosamente ${name} - ${phone} 
        <footer/>
      `,
    };

    await transporter.sendMail(mailOptions);
    return NextResponse.json(
      { message: "E-mail enviado com sucesso" },
      { status: 200 }
    );
  } catch (err) {
    console.log(err);
    return NextResponse.json(
      { message: "Erro ao enviar e-mail" },
      { status: 500 }
    );
  }
}