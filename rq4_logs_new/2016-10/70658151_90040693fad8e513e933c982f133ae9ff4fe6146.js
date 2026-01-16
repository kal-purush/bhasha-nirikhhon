var Botkit = require('./node_modules/botkit/lib/Botkit.js');


if (!process.env.token) {
    console.log('Error: Specify token in environment');
    process.exit(1);
}

var controller = Botkit.slackbot({
    debug: false
});

controller.spawn({
    token: process.env.token
}).startRTM(function(err) {
    if (err) {
        throw new Error(err);
    }
});

controller.hears(['report'], ['direct_message', 'direct_mention', 'mention'], function(bot, message) {
        console.log(message);
        messageTextArr = message.text.split(" ");
        console.log(messageTextArr[1]);
        toSayArr = [];
        for (var i = 2; i < messageTextArr.length; i++) {
            toSayArr.push(messageTextArr[i]);
        }
        text = toSayArr.join(" ");
        channel = messageTextArr[1].replace(/[<@#>\s]/g, "").split("|")[0];
        console.log("message:", text, "%");
        console.log("channel:", channel, "%");

        console.log(messageTextArr[0] == "report");
        if (messageTextArr[0] == "report") {
            bot.say({
                text: toSayArr.join(" "),
                channel: messageTextArr[1].replace(/[<@#>\s]/g, "").split("|")[0] // a valid slack channel, group, mpim, or im ID
            });
        }
    });