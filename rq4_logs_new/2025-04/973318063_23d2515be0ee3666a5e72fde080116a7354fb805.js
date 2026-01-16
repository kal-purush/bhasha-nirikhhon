const express = require('express');
const cors = require('cors');
const twilio = require('twilio');
const dotenv = require('dotenv');
const path = require('path');
const cron = require('node-cron');
const mongoose = require('mongoose');

dotenv.config();

const app = express();
const PORT = process.env.PORT || 5000;

// MongoDB connection without deprecated options
mongoose.connect(process.env.MONGO_URI)
  .then(() => console.log('✅ Connected to MongoDB'))
  .catch(err => console.error('❌ Failed to connect to MongoDB:', err));

// Twilio credentials
const accountSid = process.env.TWILIO_ACCOUNT_SID;
const authToken = process.env.TWILIO_AUTH_TOKEN;
const client = twilio(accountSid, authToken);

// Middleware
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'src')));

// Define the scheduled message model
const ScheduledMessage = mongoose.model('ScheduledMessage', new mongoose.Schema({
  name: String,
  number: String,
  message: String,
  scheduledTime: Date
}));

// Function to send WhatsApp message
async function sendWhatsAppMessage(to, body) {
  try {
    const message = await client.messages.create({
      body: body,
      from: 'whatsapp:+14155238886',  // Your Twilio sandbox WhatsApp number
      to: `whatsapp:${to}`
    });
    console.log(`✅ Message sent to ${to}: "${body}"`);
    console.log(`Message SID: ${message.sid}`);
  } catch (error) {
    console.error(`❌ Failed to send message to ${to}:`, error.message);
  }
}

// API route to schedule a message
app.post('/schedule', async (req, res) => {
  const { name, number, message, date, time } = req.body;

  if (!name || !number || !message || !date || !time) {
    return res.status(400).json({ message: '❌ Missing required fields.' });
  }

  try {
    const scheduledTime = new Date(`${date}T${time}:00`);

    if (scheduledTime < new Date()) {
      return res.status(400).json({ message: '❌ Scheduled time is in the past. Choose a future time.' });
    }

    // Create a new ScheduledMessage document and save it to the database
    const scheduledMessage = new ScheduledMessage({
      name,
      number,
      message,
      scheduledTime
    });

    await scheduledMessage.save();

    console.log(`⏳ Scheduled message for ${name} at ${scheduledTime.toISOString()}`);
    return res.json({ message: `⏳ Message scheduled for ${name} at ${scheduledTime.toISOString()}` });
  } catch (error) {
    console.error(error);
    return res.status(400).json({ message: '❌ Invalid date/time format. Please use correct formats.' });
  }
});

// Cron job that runs every minute to check scheduled messages
cron.schedule('* * * * *', async () => {
  console.log('⏰ Checking scheduled messages...');
  const now = new Date();

  // Find messages that are scheduled to be sent
  const messagesToSend = await ScheduledMessage.find({ scheduledTime: { $lte: now } });

  for (const message of messagesToSend) {
    console.log(`📤 Time to send message to ${message.name} (${message.number})`);
    await sendWhatsAppMessage(message.number, message.message);

    // Remove the sent messages from the database
    await ScheduledMessage.deleteOne({ _id: message._id });
  }
});

// Start the server
app.listen(PORT, () => {
  console.log(`🚀 Server running on port ${PORT}`);
});