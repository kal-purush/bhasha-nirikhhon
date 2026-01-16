// Import the express and flyio modules
const express = require('express');
const slackWeb = require('@slack/web-api');
require('dotenv').config()

// Create an express app
const app = express();

// Define a route for the API
app.post('/api', (req, res) => {
    console.log("Token: " + process.env.SLACK_TOKEN);
    // Create a new instance of the WebClient class with the token read from your environment variable
    const web = new slackWeb.WebClient(process.env.SLACK_TOKEN);

    
    // Call the chat.postMessage method using the WebClient
    web.chat.postMessage({
        channel: '#testtest',
        text: 'Hello from Slack!',
    })
        .then((result) => {
            // The result contains an identifier for the message, `ts`.
            console.log(`Successfully send message ${result.ts} in conversation ${result.channel}`);
            res.send('Message sent!');
        })
        .catch((error) => {
            console.error(error);
            res.send('Error sending message');
        });
    }
);
  
// Start the server
const PORT = process.env.PORT || 3000;

// Start express server
app.listen(PORT, () => {
    console.log(`Express server listening on port ${PORT}`);
});