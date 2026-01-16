const accountSid = 'ACede618fe1566ca83317d12cee231f067'; 
const authToken = 'f29c161504d4d7c8bffae7653fe00f23';
const client = require('twilio')(accountSid, authToken);

const sendSms = (phone, message) => {
    console.log(phone, message, "inside sendSms func");
    client.messages.create({
            body: message,
            from: process.env.TWILIO_PHONE_NUMBER,
            to: phone
        })
        .then(message => console.log(message.sid))
        .catch(error=>console.log(error.message))
}

module.exports = sendSms;