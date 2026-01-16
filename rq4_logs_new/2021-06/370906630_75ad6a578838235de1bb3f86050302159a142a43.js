import nodemailer from 'nodemailer'

const transporter = nodemailer.createTransport({
   host: process.env.MAIL_HOST,
   port: process.env.MAIL_PASS,
   secure: false,
   auth: {
      user: process.env.MAIL_USER,
      pass: process.env.MAIL_PASS,
   },
})

async function sendEmail({ name, email, phone, message }) {
   console.log('test', name, email)
   const emailOptions = {
      from: `${name} <${email}>`,
      to: 'roy@royanger.com',
      subject: `Contact - ${name}`,
      html: `
         <p>This message is from the contact form on roy@royanger.com</p>

         <p>Name: ${name}</p>
         <p>Email: ${email}</p>
         <p>Phone: ${phone}</p>
         <br />
         <p>Message: ${message}</p>
      `,
   }
   return transporter.sendMail(emailOptions)
}

export default async function handler(req, res) {
   if (req.method === 'POST') {
      const emailRes = await sendEmail(req.query)
      if (emailRes.messageId) {
         return res.status(200).json({ message: `Email sent successfully` })
      }

      return res.status(400).json({ message: 'Error sending email' })
   }

   return res
      .status(400)
      .json({ message: `Incorrect method: ${req.method}. Did you mean POST?` })
}