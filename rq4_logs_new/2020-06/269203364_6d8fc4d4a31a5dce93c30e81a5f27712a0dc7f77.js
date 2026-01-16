var postmark = require("postmark");

const { 
    FROM_EMAIL,
    KEY,
    TO_EMAIL,
 } = process.env

module.exports = {
    onEnd: async ({ inputs }) => {
        if (!FROM_EMAIL) {
            throw new Error('No sender email present')
          }
          if (!TO_EMAIL) {
            throw new Error('No recipient email present')
          }
          if (!KEY) {
            throw new Error('No KEY present')
          }

          const client = new postmark.ServerClient(KEY);
          const message = 'Hello Boss, we just deployed some bug fixes'
          await client.sendEmail({
            From: inputs.from || FROM_EMAIL,
            To: inputs.to || TO_EMAIL,
            Subject: "New Deploy",
            TextBody: message
          }) 
    },
}