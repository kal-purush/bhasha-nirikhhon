require("dotenv").config();
const express = require("express");
const bodyParser = require("body-parser");
const twilio = require("twilio");

const app = express();
app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.json());

// Twilio credentials
const accountSid = process.env.TWILIO_ACCOUNT_SID;
const authToken = process.env.TWILIO_AUTH_TOKEN;
const twilioClient = twilio(accountSid, authToken);
const twilioNumber = process.env.TWILIO_PHONE_NUMBER;

// Helper function to send anonymized messages
function sendMessage(fromAlias, toAlias, message, contacts) {
    const from = contacts[fromAlias];
    const to = contacts[toAlias];

    if (!from || !to) {
        throw new Error("Invalid alias used.");
    }

    return twilioClient.messages.create({
        body: `${fromAlias}: ${message}`,
        from: twilioNumber,
        to: to,
    });
}

// Handle incoming messages
app.post("/messages", async(req, res) => {
    const { sender, recipients, message, contacts } = req.body;

    if (!contacts || !contacts["Content Producer"] || !contacts["Client"] || !contacts["Project Manager"]) {
        return res.status(400).send("Contacts object must include 'Content Producer', 'Client', and 'Project Manager'.");
    }
    if (!sender || !recipients || !message) {
        return res.status(400).send("Missing required fields: sender, recipients, message.");
    }

    try {
        const promises = recipients.map((recipient) => 
            sendMessage(sender, recipient, message, contacts)
        );
        await Promise.all(promises);
        res.send("Messages sent!");
    } catch (error) {
        res.status(500).send(`Error sending messages: ${error.message}`);
    }
    
});

// Webhook for handling inbound messages
app.post("/incoming", async (req, res) => {
    const { From, Body, contacts } = req.body; 

    if (!contacts || !contacts["Content Producer"] || !contacts["Client"] || !contacts["Project Manager"]) {
        return res.status(400).send("Contacts object must include 'Content Producer', 'Client', and 'Project Manager'.");
    }

    const senderAlias = Object.keys(contacts).find(alias => contacts[alias] === From);

    if (!senderAlias) {
        return res.status(400).send("Unknown sender.");
    }

    try {
        // Forward the message to other group members
        const recipients = Object.keys(contacts).filter(alias => alias !== senderAlias);
        const promises = recipients.map(recipientAlias => 
            sendMessage(senderAlias, recipientAlias, Body, contacts)
        );
        await Promise.all(promises);

        res.send("Message forwarded to group.");
    } catch (error) {
        res.status(500).send(`Error processing message: ${error.message}`);
    }
});

// Start server
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
})