const express = require('express');
const bodyParser = require('body-parser');
const twilio = require('twilio');
require('dotenv').config();

const app = express();
const port = 5000;

const accountSid = process.env.TWILIO_ACCOUNT_SID;
const authToken = process.env.TWILIO_AUTH_TOKEN;
const client = new twilio(accountSid, authToken);

app.use(bodyParser.json());

app.post('/send-whatsapp', (req, res) => {
  const { to, message } = req.body;

  client.messages
    .create({
      body: message,
      from: 'whatsapp:+14155238886', // Número do Twilio (sandbox ou outro número habilitado para WhatsApp)
      to: `whatsapp:${to}`
    })
    .then((message) => {
      res.status(200).json({ success: true, sid: message.sid });
    })
    .catch((err) => {
      res.status(500).json({ success: false, error: err.message });
    });
});

app.listen(port, () => {
  console.log(`Servidor rodando na porta ${port}`);
});