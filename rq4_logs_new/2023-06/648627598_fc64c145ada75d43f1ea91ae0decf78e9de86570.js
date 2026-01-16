const express = require('express');
const app = express();
const cors = require('cors');
const bodyParser = require('body-parser');

const port = 5000;
const nodemailer = require('nodemailer');
app.use(bodyParser.json({'extended' : true}));
app.use(cors());
// Create a transporter
const transporter = nodemailer.createTransport({
    host: "asmtp.mail.hostpoint.ch",
    port: 465,
    secure: true,
    auth: {
      user: 'thierry@kellyburger.com', // Your email address
      pass: 'tc1Erfus' // Your email password
    }
  });

app.post("/api/send-email", (request, response) => {
const {email, message} = request.body;
console.log(email, message);
    // Define the email options
  const mailOptions = {
    from: 'ipad@kellyburger.com',
    to: 'thierry@kellyburger.com',
    subject: 'Test Email',
    text: `${message}. Sent from ${email}`
  }
  
  // Send the email
  transporter.sendMail(mailOptions, (error, info) => {
    if (error) {
      console.log('Error occurred:', error.message);
    } else {
      console.log('Email sent successfully!');
      response.sendStatus(200);
    }
  });
});

app.listen(5000, () => {
	console.log(`The backend successfully started on port ${port}!`);
});