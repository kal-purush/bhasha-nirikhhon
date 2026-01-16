const apiai = require('apiai');
const config = require('../config');
const fbService = require('../fb-service/fb-service');

const apiAiService = apiai(config.API_AI_CLIENT_ACCESS_TOKEN, {
    language: 'en',
    requestSource: 'fb'
});

function sendToApiAi(sessionIds, handleApiAiResponse, sender, text) {

    fbService.sendTypingOn(sender);
    let apiaiRequest = apiAiService.textRequest(text, {
        sessionId: sessionIds.get(sender)
    });

    apiaiRequest.on('response', (response) => {
        if (response.hasOwnProperty('result')) {
            handleApiAiResponse(sender, response);
        }
    });

    apiaiRequest.on('error', (error) => console.error(error));
    apiaiRequest.end();
}


module.exports = sendToApiAi;