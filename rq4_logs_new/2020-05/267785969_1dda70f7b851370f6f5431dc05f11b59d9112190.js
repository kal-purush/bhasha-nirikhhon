const TelegramBot = require('node-telegram-bot-api');
let music = require('musicmatch')({apikey:"52d6ace43232f2a871e699dc324ffbe8"});


// replace the value below with the Telegram token you receive from @BotFather
const token = '1209283272:AAFTDTbBxr1-JdkdlJGoYsxLPwiTfhxWRsc';

// Create a bot that uses 'polling' to fetch new updates
const bot = new TelegramBot(token, {polling: true});

// bot.onText(/\/start/, (msg) => {
    
//   bot.sendMessage(msg.chat.id, "Welcome", {
//   "reply_markup": {
//       "keyboard": [["Sample text", "Second sample"],   ["Keyboard"], ["I'm robot"]]
//       }
//   });
      
//   });

//   bot.on('message', (msg) => {
//     var Hi = "hi";
//     if (msg.text.toString().toLowerCase().indexOf(Hi) === 0) {
//         bot.sendMessage(msg.chat.id, "Hello dear user");
//     }
//     var bye = "bye";
//     if (msg.text.toString().toLowerCase().includes(bye)) {
//         bot.sendMessage(msg.chat.id, "Hope to see you around again , Bye");
//     }    
//     var robot = "I'm robot";
//     if (msg.text.indexOf(robot) === 0) {
//         bot.sendMessage(msg.chat.id, "Yes I'm robot but not in that way!")
//         bot.on('message', async (msg) => {
//           // const resp = match[1];
//           async function lyrics (resp) {
//             let text = await music.trackLyrics({track_id: resp})
//             return text
//           }
//           const eee = await lyrics(msg.text)
//           const chatId = msg.chat.id;
          
//             bot.sendMessage(chatId, eee.message.body.lyrics.lyrics_body);
//         });
//     }
//     });
// Matches "/echo [whatever]"
bot.onText(/\/echo (.+)/, (msg, match) => {
  // 'msg' is the received Message from Telegram
  // 'match' is the result of executing the regexp above on the text content
  // of the message

  const chatId = msg.chat.id;
  const resp = match[1]; // the captured "whatever"

  // send back the matched "whatever" to the chat
  bot.sendMessage(chatId, resp);
});
async function lyrics () {
  let text = await music.trackLyrics({track_id:197246819})
  return text
 }
// Listen for any kind of message. There are different kinds of
// messages.
// bot.on('message', async (msg) => {
//   const chatId = msg.chat.id;

//   // console.log(eee.message.body.lyrics.lyrics_body);

//   // send a message to the chat acknowledging receipt of their message
//   const eee = await lyrics()
  
//   bot.sendMessage(chatId, eee.message.body.lyrics.lyrics_body)
// })


bot.onText(/\/text (.+)/, async (msg, match) => {
  console.log(match);
  
  const resp = match[1];
  async function lyrics (resp) {
    let text = await music.trackLyrics({track_id: resp})
    return text
  }
  const eee = await lyrics(resp)
  const chatId = msg.chat.id;
  
    bot.sendMessage(chatId, eee.message.body.lyrics.lyrics_body);
  });


  bot.onText(/\/find (.+)/, async (msg, match) => {
    console.log(match);
    const resp = match[1];
    async function find (resp) {
      let text = await music.trackSearch({q: resp, page:1, page_size:5})
      return text
    }
    const findArr = await find(resp)
    // console.log(eee.message.body.track_list[0].track);
    
    const chatId = msg.chat.id;
    for (let index = 0; index < findArr.message.body.track_list.length; index++) {
      bot.sendMessage(chatId, `Название: ${findArr.message.body.track_list[index].track.track_name}\nИсполнитель:${findArr.message.body.track_list[index].track.artist_name}\nАльбом: ${findArr.message.body.track_list[index].track.album_name}\nId песни: ${findArr.message.body.track_list[index].track.track_id}`);
    }
    });
