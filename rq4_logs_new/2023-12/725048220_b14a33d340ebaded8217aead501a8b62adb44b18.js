import nodemailer from 'nodemailer';

export default async function handler(req, res) {
    if (req.method === 'POST') {
      const { name, email, message } = req.body;
  
      // Create a Nodemailer transporter
      const transporter = nodemailer.createTransport({
        sendmail: true,
      });
  
      try {
        // Send email
        await transporter.sendMail({
          from: 'me@alistairwilson.co.uk', // Sender's email address
          to: 'hello@alistairwilson.co.uk', // Receiver's email address
          subject: 'New Contact Form Submission',
          text: `Name: ${name}\nEmail: ${email}\nMessage: ${message}`,
        });
  
        res.status(200).json({ message: 'Email sent successfully' });
      } catch (error) {
        console.error('Error sending email:', error);
        res.status(500).json({ message: 'Failed to send email' });
      }
    } else {
      res.status(405).json({ message: 'Method Not Allowed' });
    }
  }