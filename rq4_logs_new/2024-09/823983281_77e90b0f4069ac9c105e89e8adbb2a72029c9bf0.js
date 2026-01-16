import express from 'express';
import { sendGmail } from '../utils/sendGmail.js';
import contactEmailTemplate from '../views/contactEmailTemplate.js';

const router = express.Router();

router.post('/send-email', async (req, res) => {
  const { username, email, whatsapp, content } = req.body;

  // Ensure required fields are provided
  if (!username || !email || !whatsapp || !content) {
    return res.status(400).json({ success: false, message: 'All fields are required' });
  }

  const subject = 'New Contact Form Submission';
  const replyTo = email;
  const cc = []; 
  const template = contactEmailTemplate(username, email, whatsapp,content);

  try {
    await sendGmail(subject, 'carrey.120.cc@gmail.com', replyTo, template, cc);
    res.status(200).json({ success: true, message: 'Message sent successfully!' });
  } catch (error) {
    console.error('Failed to send message:', error);
    res.status(500).json({ success: false, message: 'Failed to send message.' });
  }
});

export default router;