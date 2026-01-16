exports.run = async (bot, message, args) => {
    const req = require('../../core/https.js');
    var url = "https://some-random-api.ml/img/fox";

    req(url).then((prom) => {
        content = JSON.parse(prom);
        message.channel.send(content.link);
    });
}

exports.help = {
    name: 'fox',
    aliases: [],
    usage: "Use fox for a random fox",
    require: "There's no permission require"
}