const Discord = require('discord.js');
const { Client, MessageEmbed } = require('discord.js');
const client = new Discord.Client();

client.on('ready', () => {
    console.log(`Logged in as ${client.user.tag}!`);
  });

  client.on('message', message => {
    if (message.content === 'boog thermite') {
        const embed = new MessageEmbed()
        .setTitle("How to make the mississippi queen")
        .setDescription("https://anonfile.com/Nbyd95lbo5/NOT_thermite_txt");
        message.channel.send(embed);
    }
});

client.on('message', message => {
  if (message.content === 'boog plates') {
      const embed = new MessageEmbed()
      .setTitle("How not to get your shit rocked by a .22")
      .setDescription("The best plates as of 2020 are Overall: ShotStop Duritium GT III+, Lightweight: Infidel Lightweight III+, And protection: HighCom Guardian 4S17M IV");
      message.channel.send(embed);
  }
});

client.on('message', message => {
  if (message.content === 'boog carriers') {
      const embed = new MessageEmbed()
      .setTitle("How to hold all that sweet sweet steal of yours")
      .setDescription("Best plate carriers as of 2020 are Overall: Shellback Tactical Defender, Versatile: Barbarians MOLLE, and Custom: TacTec Plate Carrier by 5.11");
      message.channel.send(embed);
  }
});

client.on('message', message => {
  if (message.content === 'boog books') {
      const embed = new MessageEmbed()
      .setTitle("Based as shit books")
      .setDescription("https://www.murdochmurdoch.net/various/ is an archive and im too lazy to list them all, but i recommend The Art of War, The Decline and Fall of the Roman Empire, and The Turner Diaries.");
      message.channel.send(embed);
  }
});

client.on('message', message => {
  if (message.content === 'boog levels') {
      const embed = new MessageEmbed()
      .setTitle("The levels of ballistic plates")
      .setDescription("https://cdn.discordapp.com/attachments/657800344419434496/695517195811422218/nij_armoring_standard_f57ee26c-7c5e-4906-afe7-f2515e1a88f3_1024x1024.png")
      message.channel.send(embed);
  }
});

client.on('message', message => {
  if (message.content === 'boog help') {
      const embed = new MessageEmbed()
      .setTitle("What you can ask me for")
      .setDescription("My commands are Plates, Carriers, Thermite, Books, Levels, 2a, tedk, geneva, killdozer, israelicrimes, kalergi, server, Adding more as I go, Its a WIP, If you have any suggestions DM me at Vixere#9100.")
      message.channel.send(embed);
  }
});

client.on('message', message => {
  if (message.content === 'boog 2a') {
      const embed = new MessageEmbed()
      .setTitle("The Second Amendment")
      .setDescription("A well-regulated Militia, being necessary to the security of a free State, the right of the people to keep and bear Arms, shall not be infringed.")
      message.channel.send(embed);
  }
});

client.on('message', message => {
  if (message.content === 'boog tedk') {
      const embed = new MessageEmbed()
      .setTitle("Ted K, The god of based boom booms.")
      .setDescription("Theodore John Kaczynski, also known as the Unabomber, is an American domestic terrorist, anarchist, and former mathematics professor.")
      message.channel.send(embed);
  }
});

client.on('message', message => {
  if (message.content === 'boog tedk') {
      const embed = new MessageEmbed()
      .setDescription("He was a mathematics prodigy, but he abandoned his academic career in 1969 to pursue a more primitive lifestyle. Between 1978 and 1995, he killed three people and injured 23 others in an attempt to start a revolution by conducting a nationwide bombing campaign targeting people involved with modern technology.")
      message.channel.send(embed);
  }
});

client.on('message', message => {
  if (message.content === 'boog geneva') {
      const embed = new MessageEmbed()
      .setTitle("A suggestion")
      .setDescription("The Geneva Convention is the cringiest shit, but you can read about it here: https://www.icrc.org/en/war-and-law/treaties-customary-law/geneva-conventions")
      message.channel.send(embed);
  }
});

client.on('message', message => {
  if (message.content === 'boog killdozer') {
      const embed = new MessageEmbed()
      .setTitle("Treading on those who treaded on him")
      .setDescription("Marvin John Heemeyer was an American welder and an automobile muffler repair shop owner who demolished numerous buildings with a modified bulldozer in Granby, Colorado on June 4, 2004, you can watch a documentary here: https://gettvmovies.online/movie/576560")
      message.channel.send(embed);
  }
});

client.on('message', message => {
  if (message.content === 'boog kalergi') {
      const embed = new MessageEmbed()
      .setTitle("Kill the jews already please.")
      .setDescription("Read up, shits crazy. http://www.hist-chron.com/judentum-aktenlage/zionismus/op/Kalergi/ENGL-Kalergi-plan.html")
      message.channel.send(embed);
  }
});

client.on('message', message => {
  if (message.content === 'boog israelicrimes') {
      const embed = new MessageEmbed()
      .setTitle("Tie me to a missile and fire me at tel aviv")
      .setDescription("Israeli war cringe since 2014: http://palestineun.org/23-july-2014-growing-list-of-israeli-war-crimes/")
      message.channel.send(embed);
  }
});

client.on('message', message => {
  if (message.content === 'band') {
      const embed = new MessageEmbed()
      .setTitle("Listen to trivium already")
      .setDescription("https://open.spotify.com/artist/278ZYwGhdK6QTzE3MFePnP?si=GsMlLksCSMySw62Na_6sEw")
      message.channel.send(embed);
  }
});

client.on('message', message => {
  if (message.content === 'boog server') {
      const embed = new MessageEmbed()
      .setTitle("Join Vixere's Server")
      .setDescription("https://discord.gg/mMeDGTj")
      message.channel.send(embed);
  }
});

client.login('Njk1NDc4MDY0OTA1NDUzNjEw.XobLCw.w8KyXb8fOAV7tL-PSfj7soXB8bM');