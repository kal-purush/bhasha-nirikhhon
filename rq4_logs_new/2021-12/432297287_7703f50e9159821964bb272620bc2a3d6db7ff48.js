require('dotenv').config();

const accountSid = process.env.TWILIO_ACCOUNT_SID;
const authToken = process.env.TWILIO_AUTH_TOKEN;
const client = require('twilio')(accountSid, authToken);

module.exports = customerNumber => {

  client.messages
    .create({
       body: 'This is a message from UberLabs',
       from: '+16137773003',
       to: customerNumber
     })
    .then(message => console.log(message.sid));
};