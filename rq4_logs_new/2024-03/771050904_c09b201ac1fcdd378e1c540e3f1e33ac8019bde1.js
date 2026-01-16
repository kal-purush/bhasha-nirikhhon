const Discord = require('discord.js-selfbot-v13');
const client = new Discord.Client({
  readyStatus: false,
  checkUpdate: false
});

const keepAlive = require('./server.js');
keepAlive();

function formatTime() { //Credits to himika#0001 and never#0001
  const date = new Date();
  const options = {
    timeZone: 'Asia/Seoul', //https://www.zeitverschiebung.net/en/ and find your city and enter here
    hour12: true,
  };
  return new Intl.DateTimeFormat('ko-KR', options).format(date);
}

client.on('ready', async () => {
  console.clear();
  console.log(`${client.user.tag} - rich presence started!`);

  const r = new Discord.RichPresence()
    .setApplicationId('1196606386470141962')
    .setType('STREAMING')
    .setURL('https://www.youtube.com/watch?v=dQw4w9WgXcQ') //Must be a youtube video link 
    .setState('24H Online')
    .setName('mrnekrozyt')
    .setDetails(`You Don't have MoM [${formatTime()}]`)
    .setStartTimestamp(Date.now())
    .setAssetsLargeImage('https://cdn.discordapp.com/attachments/1203073228059189300/1217133707803230289/f5abba1882742bf6.gif?ex=6602eb19&is=65f07619&hm=5490b502fc2e908485ca9f411467a532e0ec8f8feea16784b1feb89fd00b41c0&') //You can put links in tenor or discord and etc.
    .setAssetsLargeText('니엄') //Text when you hover the Large image
    .setAssetsSmallImage("https://cdn.discordapp.com/attachments/1203073228059189300/1217135072386220104/2.gif?ex=6602ec5e&is=65f0775e&hm=0c0be1ae1899023bf0c3c8b9a501340554258cbb8d5f57f9d8ee77e800f97963&") //You can put links in tenor or discord and etc.
    .setAssetsSmallText('Small Text') //Text when you hover the Small image
    .addButton('눌러보세요', 'https://youareanidiot.cc/')

  client.user.setActivity(r);
  client.user.setPresence({ status: "dnd" }); //dnd, online, idle, offline

  let prevTime = null;
  setInterval(() => {
    const newTime = formatTime();
    if (newTime !== prevTime) {
      const newDetails = `You Don't have MoM [${newTime}]`;
      r.setDetails(newDetails);
      client.user.setActivity(r);
      prevTime = newTime;
    }
  }, 1000); // Update every second
});

const mySecret = process.env['TOKEN'];
client.login("OTg3NTgyMTkwNzMwNzUyMDcw.Gtfr4c.XuUkrsQMqWcHQKV_-dnEqUYNjb7L_djNucRm3o");