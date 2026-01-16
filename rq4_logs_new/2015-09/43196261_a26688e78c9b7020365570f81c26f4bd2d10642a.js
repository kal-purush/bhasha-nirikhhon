var Slack = require('slack-client');

function SlackConnector(bot, key){
  var slack = new Slack(key, true, true),
      slackReady = false;

  this.sendMessage = function(message, channel){
    if(slackReady) slack.getChannelGroupOrDMByID(channel).send(message);
  };

  this.handleMessage = function(){
    // initialize the handler
    slack.on('message', function(message) {
      var type = message.type,
      user = slack.getUserByID(message.user),
      time = message.ts,
      text = message.text,
      response = '';

      if (type === 'message' && user) {
        bot.processMessage(text, message.channel, user.name, {message: message});
      }
    });
  };

  slack.on('open', function() {
    var slackReady = true;
  });

  return this;
}

module.exports = SlackConnector;