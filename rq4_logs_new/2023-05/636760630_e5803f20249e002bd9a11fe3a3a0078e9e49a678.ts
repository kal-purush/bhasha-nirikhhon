import * as express from 'express';
import * as cors from 'cors';
import * as bodyParser from 'body-parser';
import * as nodemailer from 'nodemailer';
import * as dotenv from 'dotenv';
import * as mongoose from 'mongoose';

dotenv.config();

// Connect to MongoDB
mongoose.connect(process.env.DB_CONNECTION_STRING || '', {
  useUnifiedTopology: true,
} as mongoose.ConnectOptions);



// Define Mongoose schema for contact messages
const contactSchema = new mongoose.Schema({
  name: String,
  email: String,
  subject: String,
  message: String
});

const Contact = mongoose.model('Contact', contactSchema);

const app = express();

// Middleware
app.use(cors());
app.use(bodyParser.json());

app.post('/send', (req, res) => {
  const data = req.body;

  // Save contact message in the database
  const contact = new Contact({
    name: data.name,
    email: data.email,
    subject: data.subject,
    message: data.message
  });

  contact.save()
    .then(() => {
      // Send email
      const smtpTransport = nodemailer.createTransport({
        service: 'Gmail',
        port: 465,
        auth: {
          user: process.env.EMAIL_ADDRESS,
          pass: process.env.EMAIL_PASSWORD
        }
      });

      const mailOptions = {
        from: data.email,
        to: 'herve.nguetsop@gmail.com',
        subject: `Message de ${data.name}`,
        html: `
          <p>${data.name}</p>
          <p>${data.email}</p>
          <p>${data.subject}</p>
          <p>${data.message}</p>
        `
      };

      smtpTransport.sendMail(mailOptions,
        (error: Error | null, response: nodemailer.SentMessageInfo) => {
          if(error) {
            res.send(error)
          } else {
            res.send('Success')
          }
          smtpTransport.close();
        });
    })
    .catch((error: Error) => {
      res.send(error);
    });
});

app.listen(5000, ()=> {
  console.log('Server is running on port 5000');
});