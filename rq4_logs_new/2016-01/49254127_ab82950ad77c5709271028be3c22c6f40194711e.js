require('prototypes');
var TelegramBot = require('node-telegram-bot-api');
var geocoder = require('geocoder');


var token = 'token';
var bot = new TelegramBot(token, {
  polling: true
});

var code = 'telegram';
var clients = new Array();
var resultMultitest =
  'http://www.multitest.ua/coordinates/internet-v-kvartiru/';
var textCaption = 'Я multitest бот!';
var textWelcome = 'Привет я multitest bot! Где Вы хотите найти интернет?';
var textError =
  'Упс, мы не смогли распознать адрес. Введите свой адрес, например Киев, Николая Бажана просп. 32';
var textResult =
  'Ваш адрес %s? Тогда нажмите по ссылке и выбирайте лучший тариф!';


bot.on('message', function(msg) {
  var chatId = msg.chat.id;

  if (!clients[msg.from.id]) {
    bot.sendMessage(chatId,
      textWelcome, {
        caption: textCaption
      });
    clients[msg.from.id] = true;
  } else {
    geocoder.geocode(msg.text, function(err, data) {
      if (data.status == 'ZERO_RESULTS') {
        bot.sendMessage(chatId,
          textError, {
            caption: textCaption
          });
      } else {
        try {
          address = data.results[0].formatted_address;
          lat = data.results[0].geometry.location.lat;
          lng = data.results[0].geometry.location.lng;
          bot.sendMessage(chatId,
            textResult
            .format(address), {
              caption: textCaption
            });
          bot.sendMessage(chatId,
            '%s?lat=%s&lng=%s&address_text=%s&code=%s'.format(
              resultMultitest, lat, lng, encodeURIComponent(address),
              code), {
              caption: textCaption
            });
        } catch (e) {
          bot.sendMessage(chatId,
            textError, {
              caption: textCaption
            });
        }
      }
    });
  }
});