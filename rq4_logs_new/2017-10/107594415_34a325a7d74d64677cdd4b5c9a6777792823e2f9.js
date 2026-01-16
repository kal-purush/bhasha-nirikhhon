'use strict';

const bot = require('./services/bot.js');

module.exports.chatbot = (event, context, callback) => {
  if (event.method === 'GET') {
    // facebook app verification
    if (event.query['hub.verify_token'] === process.env.FB_APP_TOKEN && event.query['hub.challenge']) {
      return callback(null, parseInt(event.query['hub.challenge']));

    } else {
      return callback(new Error('[403] Invalid token'));
    }
  }

  if (event.method === 'POST') {
    event.body.entry.map((entry) => {
      entry.messaging.map((messagingItem) => {
        const senderId = messagingItem.sender.id;

        // handle postback messages
        if (messagingItem.postback && messagingItem.postback.payload) {
          // Do something with messagingItem.postback.payload
          return callback(null, {
            statusCode: 200,
            body: JSON.stringify({
              message: 'Handled postback',
              input: event,
            }),
          });

        // handle 'message type' message
        } else if (messagingItem.message) {
          const msg = messagingItem.message;

          // Handle quick message (help only)
          if (msg.quick_reply && msg.quick_reply.payload === 'INIT_HELP') {
            bot.notifyProcessing(senderId);
            bot.sendHelpMessage(senderId).then(function () {
              return callback(null, {
                statusCode: 200,
                body: JSON.stringify({
                  message: 'Handled help',
                  input: event,
                }),
              }
            );
          }).catch(function (err) {
            return callback(err);
          });

          // Handle text message
          } else if (msg.text) {
            const text = msg.text;
            bot.notifyProcessing(senderId);

            // Do something here, or else send a default message like this one:
            bot.sendPuzzledApology(senderId).then(function () {
              return callback(null, {
                  statusCode: 404,
                  body: JSON.stringify({
                    message: 'Unable to handle request',
                    input: event,
                  })
                }
              );
            }).catch(function (err) {
              return callback(err);
            });
          }
        }
      });
    });
  } else {
    const response = {
      statusCode: 400,
      body: JSON.stringify({
        message: 'Bad Request',
        input: event,
      }),
    };

    return callback(null, response);
  }
};