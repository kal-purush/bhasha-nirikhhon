const Discord = require('discord.js');
const client = new Discord.Client();

client.on('ready', () => {
  console.log('Todo listo!');
});

var responseObject = {
  "Yare, yare." : "Desu.",
  "Yare, yare": "Desu.",
  "Yare yare": "Desu.",
  "yare, yare.": "Desu.",
  "yare yare": "Desu.",
  "Hoo!": "Haa!",
  "Say my name.": "You're Heisenberg...",
  "Nah": "Nah, son.",
  "What is love?": "Baby, don't hurt me.",
  "fierro": "¡No fierro!",
  "ayy": "Ayy, lmao!",
  "wat": "Say what?",
  "lol": "roflmaotntpmp",
  "Poni": "❤",
  "Pony": "❤",
  "pony": "❤"
};
client.on('message', (message) => {
  if(responseObject[message.content]) {
    message.channel.sendMessage(responseObject[message.content]);
  }
});

client.on('message', message => {
  if (message.content === ':kappa:') {
    message.reply('¡no kappa!');
  }
  if (message.content === ':v') {
    message.reply('me has decepcionado...');
  }
  if (message.content === ":'v") {
    message.reply('me has decepcionado...');
  }
  if (message.content === '¿Qué avatar tengo?') {
    // send the user's avatar URL
    message.reply(message.author.avatarURL);
  }
  if (message.content === "Fuuka, di mi nombre.") {
    // send the user's avatar URL
    message.channel.sendMessage(`Eres ${message.author.username}.`);
  }
    let prefix = "!";
  // Exit and stop if it's not there
  if(!message.content.startsWith(prefix)) return;

  if (message.content.startsWith(prefix + "ayuda")) {
    message.channel.sendMessage('Estos son los comandos actuales:\n"!ayuda": Muestra esta ayuda.\n"!8ball" + pregunta: Trataré de responder a tus preguntas. Ten en cuenta que la respuesta sólo pueder ser sí o no. Por favor, formula tus preguntas adecuadamente.\n"!moneda": Lanzaré una moneda al aire por ti y te diré el resultado.\n"!dado6": Tiraré un dado de seis caras.');
  }
  if (message.content.startsWith(prefix + "otp")) {
    message.channel.sendMessage("Eres mi vida, ¡te amo, Poni! ❤");
  }
});


client.on('message', message => {
    let prefix = "!";
  // Exit and stop if it's not there
  if(!message.content.startsWith(prefix)) return;

  if (message.content.startsWith(prefix + "8ball")) {
	var r_text = new Array ();
	r_text[0] = "¡Ni lo dudes!";
	r_text[1] = "¡Claro que sí!";
	r_text[2] = "Creo que sí.";
	r_text[3] = "Eso espero.";
	r_text[4] = "Podría ser.";
	r_text[5] = "Quizás...";
	r_text[6] = "Eh... lo siento, no lo sé.";
	r_text[7] = "Será mejor no decírtelo ahora.";
	r_text[8] = "Hoy no.";
	r_text[9] = "Espero que no...";
	r_text[10] = "Es poco probable.";
	r_text[11] = "No cuentes con ello.";
	r_text[12] = "Lo dudo...";
	r_text[13] = "Definitivamente no.";
	r_text[14] = "*blush* Sí...";
	r_text[15] = "¡N-No, ¿cómo crees?!";
	r_text[16] = "*gasp* ¡No me preguntes eso...!";
	var i = Math.floor(14*Math.random())
	var i = r_text[i];
    message.channel.sendMessage(i);
  }
});

client.on('message', message => {
    let prefix = "!";
  // Exit and stop if it's not there
  if(!message.content.startsWith(prefix)) return;

  if (message.content.startsWith(prefix + "moneda")) {
	var r_text = new Array ();
	r_text[0] = "Cayó cara.";
	r_text[1] = "Es sello.";
	var i = Math.floor(2*Math.random())
	var i = r_text[i];
    message.channel.sendMessage(i);
  }
});

client.on('message', message => {
    let prefix = "!";
  // Exit and stop if it's not there
  if(!message.content.startsWith(prefix)) return;

  if (message.content.startsWith(prefix + "dado6")) {
	var r_text = new Array ();
	r_text[0] = "1, como el as.";
	r_text[1] = "2, tsumi y batsu.";
	r_text[2] = "3, el primer número primo impar.";
	r_text[3] = "4, el número de la muerte...";
	r_text[4] = "5, como las puntas de una estrella.";
	r_text[5] = "6, como junio.";
	var i = Math.floor(6*Math.random())
	var i = r_text[i];
    message.channel.sendMessage(i);
  }
});


client.on("guildMemberAdd", (guild, member) => {
    console.log(`New User "${member.user.username}" has joined "${guild.name}"` );
    guild.defaultChannel.sendMessage(`${member.user.username} se unió al grupo.`);
});

var loginKey = PROCESS.ENV.KEY;

client.login(loginKey);