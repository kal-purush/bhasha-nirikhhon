var Botkit = require('../node_modules/botkit/lib/Botkit.js');

var stackPush = function(bot, message, name) {
  controller.storage.channels.get(message.channel, function(err, channel_data) {
    if (!channel_data) {
      channel_data = {stack: []};
    }
    stack = channel_data.stack;
    if (stack.indexOf(name) === -1) {
      stack.push(name);
      controller.storage.channels.save({id: message.channel, stack: stack}, function(err) {
        bot.reply(message, '(' + stack + ')');
      });
    } else {
      bot.reply(message, name + ' already in stack');
    }
  });
}

var stackShift = function(bot, message, name) {
  controller.storage.channels.get(message.channel, function(err, channel_data) {
    if (!channel_data) {
      channel_data = {stack: []};
    }
    stack = channel_data.stack;
    if (stack.indexOf(name) === -1) {
      bot.reply(message, name + ' not in stack');
    } else {
      stack.splice(stack.indexOf(name), 1);
      controller.storage.channels.save({id: message.channel, stack: stack}, function(err) {
        bot.reply(message, '(' + stack + ')');
      });
    }
  });
}

var controller = Botkit.slackbot({
  debug: false,
  json_file_store: 'stackbot.json'
});

var bot = controller.spawn({
  token: process.env.token
}).startRTM();

controller.hears('help', 'direct_message,direct_mention', function(bot, message) {
  bot.reply(message, '@stack help - display this message\nstack <name> - add <name> to stack\nstack me - add myself to stack\nunstack <name> - remove <name> from stack\nunstack me - remove myself from stack\n@stack next - pull the next person from the stack');
});
  
controller.hears('^stack$', 'ambient', function(bot, message) {
  controller.storage.channels.get(message.channel, function(err, channel_data) {
    if (!channel_data) {
      channel_data = {stack: []};
    }
    stack = channel_data.stack;
    bot.reply(message, '(' + stack + ')');
  });
});

controller.hears('^stack (.*)', 'ambient', function(bot, message) {
  // "stack name" means stack name
  var name = message.match[1];
  if (name === 'me') {
    // look up user's name
    var options = {
      user: message.user,
      token: process.env.token
    }
 
    bot.api.users.info(options, function (err, response) {
      stackPush(bot, message, response.user.name);
    });
  } else {
    stackPush(bot, message, name);
  }
});

controller.hears('^unstack (.*)', 'ambient', function(bot, message) {
  // "unstack name" means remove name from stack
  var name = message.match[1];
  if (name === 'me') {
    // look up user's name
    var options = {
      user: message.user,
      token: process.env.token
    }
 
    bot.api.users.info(options, function (err, response) {
      stackShift(bot, message, response.user.name);
    });
  } else {
    stackShift(bot, message, name);
  }
});

controller.hears('^next', 'direct_message,direct_mention', function(bot, message) {
  // "stack next" means get from stack
  controller.storage.channels.get(message.channel, function(err, channel_data) {
    if (!channel_data) {
      channel_data = {stack: []};
    }
    stack = channel_data.stack;
    if (stack.length > 0) {
      user = stack.shift();
      controller.storage.channels.save({id: message.channel, stack: stack}, function(err) {
        bot.reply(message, '@' + user + ' (' + stack + ')');
      });
    } else {
      bot.reply(message, 'empty');
    }
  });
});