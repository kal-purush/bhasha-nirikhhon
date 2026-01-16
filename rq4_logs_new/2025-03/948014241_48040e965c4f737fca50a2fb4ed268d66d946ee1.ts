import nodemailer from 'nodemailer'

export const sendVerificationEmail = async (email: string, token: string) => {
  const transporter = nodemailer.createTransport({
    service: 'gmail',
    auth: {
      user: process.env.EMAIL_USER,
      pass: process.env.EMAIL_PASS
    }
  })

  const verificationLink = `${process.env.CLIENT_URL}/verify-email?token=${token}`

  const mailOptions = {
    from: process.env.EMAIL_USER,
    to: email,
    subject: 'Xác nhận tài khoản',
    html: `<p>Vui lòng nhấn vào liên kết sau để xác nhận tài khoản của bạn:</p>
           <a href="${verificationLink}">Xác nhận tài khoản</a>`
  }

  await transporter.sendMail(mailOptions)
}