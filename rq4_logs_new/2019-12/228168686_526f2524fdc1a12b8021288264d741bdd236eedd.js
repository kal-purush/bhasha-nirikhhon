const Discord = require('discord.js');
const client = new Discord.Client();

client.on("message", message =>{
//السطر ال تحت لمنع تكرار انشاء الرتب لو عاوز تغير اسم الرتب غيرها وحط اسمها فى السطر ال تحت كمان علشان ميحصلش سبام
let roles = message.guild.roles.find(all=> all.name === "⇁『KiNG 』‏‏༄  ❥", "⇁『LEADER』‏‏༄  ❥", "⇁『BiG BOSS 』‏‏༄  ❥", "⇁『CAPTAIN 』‏‏༄  ❥", "⇁『SERGEANT 』‏‏༄  ❥",
"⇁『youtuber 』‏‏༄  ❥", "⇁『ACTIVE 』‏‏༄  ❥");//Baron#1500
if(message.content.startsWith(prefix + "roles")) {//Baron#1500
  if(message.author.bot) return;//Baron#1500
  if(roles) return message.reply('**الرتب موجوده بالفعل**')//Baron#1500
  //دى بقا الرتب ال انت عاوزو يعملها 
  let roleking = message.guild.roles.find(r => r.name === "⇁『KiNG 』‏‏༄  ❥");//Baron#1500
  if(!roleking) {
  message.guild.createRole({//Baron#1500
  name: '⇁『KiNG 』‏‏༄  ❥',
  color: 'RANDOM',
  position: (1),
  hoist: (true),
  permissions: 'ADMINISTRATOR'
})
  }
let roleleader = message.guild.roles.find(r => r.name === "⇁『LEADER』‏‏༄  ❥");//Baron#1500
if(!roleleader) {
 message.guild.createRole({
    name: '⇁『LEADER』‏‏༄  ❥',
    color: 'RANDOM',
    position: (2),//Baron#1500
    hoist: (true),
    permissions: ['CREATE_INSTANT_INVITE', 'KICK_MEMBERS', 'BAN_MEMBERS', 'MANAGE_CHANNELS', 'ADD_REACTIONS', 'VIEW_AUDIT_LOG', 'VIEW_CHANNEL', 'READ_MESSAGES', 'SEND_MESSAGES',
        'SEND_TTS_MESSAGES', 'MANAGE_MESSAGES', 'EMBED_LINKS', 'ATTACH_FILES', 'READ_MESSAGE_HISTORY', 'MENTION_EVERYONE', 'CONNECT', 'SPEAK', 'MUTE_MEMBERS', 'DEAFEN_MEMBERS',
         'MOVE_MEMBERS', 'USE_VAD', 'CHANGE_NICKNAME', 'MANAGE_NICKNAMES'],
})
}
let rolebigboss = message.guild.roles.find(r => r.name === "⇁『BiG BOSS 』‏‏༄  ❥");//Baron#1500
if(!rolebigboss) {
  message.guild.createRole({
    name: '⇁『BiG BOSS 』‏‏༄  ❥',
    color: 'RANDOM',
     position: (3),
     hoist: (true),
    permissions: ['CREATE_INSTANT_INVITE', 'KICK_MEMBERS', 'ADD_REACTIONS', 'VIEW_CHANNEL', 'READ_MESSAGES', 'SEND_MESSAGES', 'MANAGE_MESSAGES', 'EMBED_LINKS', 'ATTACH_FILES', 'CONNECT', 'SPEAK', 'MUTE_MEMBERS', 'DEAFEN_MEMBERS',
    'READ_MESSAGE_HISTORY', 'MENTION_EVERYONE', 'MOVE_MEMBERS', 'USE_VAD', 'CHANGE_NICKNAME', 'MANAGE_NICKNAMES']
})
}
let rolecaptain = message.guild.roles.find(r => r.name === "⇁『CAPTAIN 』‏‏༄  ❥");//Baron#1500
if(!rolecaptain) {
message.guild.createRole({
    name: '⇁『CAPTAIN 』‏‏༄  ❥',
    color: 'RANDOM',
    postion: (4),
    hoist: (true),
    permissions: ['VIEW_CHANNEL', 'READ_MESSAGES', 'SEND_MESSAGES', 'MANAGE_MESSAGES', 'EMBED_LINKS', 'ATTACH_FILES', 'CONNECT', 'SPEAK', 'MUTE_MEMBERS', 'DEAFEN_MEMBERS',
    'READ_MESSAGE_HISTORY', 'MENTION_EVERYONE', 'MOVE_MEMBERS', 'USE_VAD', 'CHANGE_NICKNAME']
})
}
let rolesergant = message.guild.roles.find(r => r.name === "⇁『SERGEANT 』‏‏༄  ❥");//Baron#1500
if(!rolesergant) {
 message.guild.createRole({
    name: '⇁『SERGEANT 』‏‏༄  ❥',
    color: 'RANDOM',//Baron#1500
    postion: (5),
    hoist: (true),//Baron#1500
    permissions: ['VIEW_CHANNEL', 'READ_MESSAGES', 'SEND_MESSAGES', 'MANAGE_MESSAGES', 'EMBED_LINKS', 'ATTACH_FILES', 'CONNECT', 'SPEAK', 'MUTE_MEMBERS',
    'READ_MESSAGE_HISTORY', 'MOVE_MEMBERS', 'USE_VAD', 'CHANGE_NICKNAME']
})
}//Baron#1500
let roleyoutuber = message.guild.roles.find(r => r.name === "⇁『youtuber 』‏‏༄  ❥");
if(!roleyoutuber) {//Baron#1500
  message.guild.createRole({
    name: '⇁『youtuber 』‏‏༄  ❥',
    color: 'RANDOM',
    postion: (6),
    hoist: (true),//Baron#1500
    permissions: ['VIEW_CHANNEL', 'READ_MESSAGES', 'SEND_MESSAGES', 'EMBED_LINKS', 'ATTACH_FILES', 'CONNECT', 'SPEAK',
    'READ_MESSAGE_HISTORY', 'MOVE_MEMBERS', 'USE_VAD', 'CHANGE_NICKNAME', 'ADD_REACTIONS']
})
}
let roleactive = message.guild.roles.find(r => r.name === "⇁『ACTIVE 』‏‏༄  ❥");//Baron#1500
if(!roleactive) {//Baron#1500
 message.guild.createRole({
  name: '⇁『ACTIVE 』‏‏༄  ❥',
  color: 'RANDOM',
  postion: (7),
  hoist: (true),//Baron#1500
  permissions: ['VIEW_CHANNEL', 'READ_MESSAGES', 'SEND_MESSAGES', 'EMBED_LINKS', 'ATTACH_FILES', 'CONNECT', 'SPEAK',//Baron#1500
  'READ_MESSAGE_HISTORY', 'MOVE_MEMBERS', 'USE_VAD', 'CHANGE_NICKNAME', 'ADD_REACTIONS']//Baron#1500
})
message.guild.createRole({//Baron#1500
  name: 'Fortnite',
  color: 'RANDOM',//Baron#1500
  postion: (7),
  permissions: ['VIEW_CHANNEL', 'READ_MESSAGES', 'SEND_MESSAGES', 'EMBED_LINKS', 'ATTACH_FILES', 'CONNECT', 'SPEAK',//Baron#1500
  'READ_MESSAGE_HISTORY', 'MOVE_MEMBERS', 'USE_VAD', 'CHANGE_NICKNAME', 'ADD_REACTIONS']//Baron#1500
})
message.guild.createRole({//Baron#1500
  name: 'playerunknowns',
  color: 'RANDOM',
  postion: (7),
  permissions: ['VIEW_CHANNEL', 'READ_MESSAGES', 'SEND_MESSAGES', 'EMBED_LINKS', 'ATTACH_FILES', 'CONNECT', 'SPEAK',
  'READ_MESSAGE_HISTORY', 'MOVE_MEMBERS', 'USE_VAD', 'CHANGE_NICKNAME', 'ADD_REACTIONS']//Baron#1500
})//Baron#1500
message.guild.createRole({
  name: 'counter-strike',
  color: 'RANDOM',//Baron#1500
  postion: (7),
  permissions: ['VIEW_CHANNEL', 'READ_MESSAGES', 'SEND_MESSAGES', 'EMBED_LINKS', 'ATTACH_FILES', 'CONNECT', 'SPEAK',
  'READ_MESSAGE_HISTORY', 'MOVE_MEMBERS', 'USE_VAD', 'CHANGE_NICKNAME', 'ADD_REACTIONS']
})//Baron#1500
message.guild.createRole({
  name: 'creative-destruction',//Baron#1500
  color: 'RANDOM',
  postion: (7),
  permissions: ['VIEW_CHANNEL', 'READ_MESSAGES', 'SEND_MESSAGES', 'EMBED_LINKS', 'ATTACH_FILES', 'CONNECT', 'SPEAK',
  'READ_MESSAGE_HISTORY', 'MOVE_MEMBERS', 'USE_VAD', 'CHANGE_NICKNAME', 'ADD_REACTIONS']
})
message.guild.createRole({//Baron#1500
  name: 'overwatch',
  color: 'RANDOM',
  postion: (7),//Baron#1500
  permissions: ['VIEW_CHANNEL', 'READ_MESSAGES', 'SEND_MESSAGES', 'EMBED_LINKS', 'ATTACH_FILES', 'CONNECT', 'SPEAK',
  'READ_MESSAGE_HISTORY', 'MOVE_MEMBERS', 'USE_VAD', 'CHANGE_NICKNAME', 'ADD_REACTIONS']
})//Baron#1500
message.guild.createRole({
  name: 'minecraft',//Baron#1500
  color: 'RANDOM',
  postion: (7),//Baron#1500
  permissions: ['VIEW_CHANNEL', 'READ_MESSAGES', 'SEND_MESSAGES', 'EMBED_LINKS', 'ATTACH_FILES', 'CONNECT', 'SPEAK',
  'READ_MESSAGE_HISTORY', 'MOVE_MEMBERS', 'USE_VAD', 'CHANGE_NICKNAME', 'ADD_REACTIONS']
})//Baron#1500
message.guild.createRole({//Baron#1500
  name: 'league-of-legends',
  color: 'RANDOM',//Baron#1500
  postion: (7),
  permissions: ['VIEW_CHANNEL', 'READ_MESSAGES', 'SEND_MESSAGES', 'EMBED_LINKS', 'ATTACH_FILES', 'CONNECT', 'SPEAK',
  'READ_MESSAGE_HISTORY', 'MOVE_MEMBERS', 'USE_VAD', 'CHANGE_NICKNAME', 'ADD_REACTIONS']
})//Baron#1500
message.channel.send(message.member + '**جارى انشاء الرتب**').then((m)=> {
  setTimeout(() => {//Baron#1500
    m.edit('**تم انشاء الرتب بنجاح**')
  }, 3000);//Baron#1500
})
}//Baron#1500
}
})//Baron#1500
Baron.on("message", message =>{
  let roleyoutuber = message.guild.roles.find(r => r.name === "⇁『youtuber 』‏‏༄  ❥");
  let rolepubg = message.guild.roles.find(r => r.name === "playerunknowns");
  let rolecsgo = message.guild.roles.find(r => r.name === "counter-strike");
  let rolecd = message.guild.roles.find(r => r.name === "creative-destruction");
  let roleow = message.guild.roles.find(r => r.name === "overwatch");
  let rolemc = message.guild.roles.find(r => r.name === "minecraft");
  let rolelol = message.guild.roles.find(r => r.name === "league-of-legends");
  let rolefortnite = message.guild.roles.find(r => r.name === "Fortnite");
//Baron#1500
  let roles = message.guild.roles.find(all=> all.name === "⇁『KiNG 』‏‏༄  ❥", "⇁『LEADER』‏‏༄  ❥", "⇁『BiG BOSS 』‏‏༄  ❥", "⇁『CAPTAIN 』‏‏༄  ❥", "⇁『SERGEANT 』‏‏༄  ❥",
"⇁『youtuber 』‏‏༄  ❥", "⇁『ACTIVE 』‏‏༄  ❥");//Baron#1500
  if(message.content.startsWith(prefix + "channels")) {
if(!roles) return message.reply("**من فضلك قم بانشاء الرتب اولا اكتب $roles**")//Baron#1500
if(roles) {//Baron#1500
message.reply("**جارى انشاء الرومات**").then((c)=> {//Baron#1500
setTimeout(() => {
  c.edit("**تم انشاء الرومات بنجاح**")//Baron#1500
}, 10000);
})
message.guild.createChannel(`${message.guild.name}-TEXT` , 'category').then(tb => {//Baron#1500
  message.guild.createChannel('welcome' , 'text').then(nws => {
    nws.setParent(tb);
    nws.overwritePermissions(message.guild.id, {
      SEND_MESSAGES: false,
      MENTION_EVERYONE: false//Baron#1500
})
  })
  message.guild.createChannel('info' , 'text').then(inf => {//Baron#1500
    inf.setParent(tb);//Baron#1500
    inf.overwritePermissions(message.guild.id, {//Baron#1500
      SEND_MESSAGES: false,
      MENTION_EVERYONE: false//Baron#1500
})
  })
    message.guild.createChannel('news' , 'text').then(nws => {//Baron#1500
      nws.setParent(tb);
      nws.overwritePermissions(message.guild.id, {//Baron#1500
        SEND_MESSAGES: false,
        MENTION_EVERYONE: false//Baron#1500
})
    })//Baron#1500
      message.guild.createChannel('chat' , 'text').then(cht => {//Baron#1500
        cht.setParent(tb);
        cht.overwritePermissions(message.guild.id, {
          MENTION_EVERYONE: false//Baron#1500
  })
      })
        message.guild.createChannel('bot-commands' , 'text').then(cmd => {//Baron#1500
          cmd.setParent(tb);
          cmd.overwritePermissions(message.guild.id, {//Baron#1500
            MENTION_EVERYONE: false//Baron#1500
    })
        })
          message.guild.createChannel('youtubers' , 'text').then(yt => {//Baron#1500
            yt.setParent(tb);//Baron#1500
            yt.overwritePermissions(roleyoutuber, {
              SEND_MESSAGES: true
          })
          yt.overwritePermissions(message.guild.id, {//Baron#1500
            SEND_MESSAGES: false,
            MENTION_EVERYONE: false
          })
          })//Baron#1500
          message.guild.createChannel('pic' , 'text').then(pic => {//Baron#1500
            pic.setParent(tb);
            pic.overwritePermissions(message.guild.id, {
              MENTION_EVERYONE: false
      })
          })
            message.guild.createChannel('cut-tweet' , 'text').then(cut => {
              cut.setParent(tb);
              cut.overwritePermissions(message.guild.id, {
                MENTION_EVERYONE: false
        })//Baron#1500
            })
  })
  message.guild.createChannel(`Games-Chat` , 'category').then(tb => {//Baron#1500
  message.guild.createChannel('Fortnite' , 'text').then(wlc => {
    wlc.setParent(tb);
    wlc.overwritePermissions(message.guild.id, {
            SEND_MESSAGES: false,
            MENTION_EVERYONE: false
  })//Baron#1500
  wlc.overwritePermissions(rolefortnite, {
    SEND_MESSAGES: true,
    MENTION_EVERYONE: false
})
  })//Baron#1500
  message.guild.createChannel('playerunknowns' , 'text').then(ch => {//Baron#1500
    ch.setParent(tb);
    ch.overwritePermissions(message.guild.id, {//Baron#1500
      SEND_MESSAGES: false,
      MENTION_EVERYONE: false
})
ch.overwritePermissions(rolepubg, {//Baron#1500
  SEND_MESSAGES: true,
  MENTION_EVERYONE: false//Baron#1500
})
  })
    message.guild.createChannel('counter-strike' , 'text').then(ch => {//Baron#1500
      ch.setParent(tb);
      ch.overwritePermissions(message.guild.id, {
        SEND_MESSAGES: false,//Baron#1500
        MENTION_EVERYONE: false
})
ch.overwritePermissions(rolecsgo, {
  SEND_MESSAGES: true,
  MENTION_EVERYONE: false
})//Baron#1500
    })
      message.guild.createChannel('creative-destruction' , 'text').then(ch => {//Baron#1500
        ch.setParent(tb);
        ch.overwritePermissions(rolecd, {
          SEND_MESSAGES: true,//Baron#1500
          MENTION_EVERYONE: false
      })
      ch.overwritePermissions(message.guild.id, {
        SEND_MESSAGES: false,
        MENTION_EVERYONE: false
      })//Baron#1500
      })
        message.guild.createChannel('overwatch' , 'text').then(ch => {
          ch.setParent(tb);
          ch.overwritePermissions(roleow, {
            SEND_MESSAGES: true,
            MENTION_EVERYONE: false
        })
        ch.overwritePermissions(message.guild.id, {
          SEND_MESSAGES: false,
          MENTION_EVERYONE: false
        })
        })
          message.guild.createChannel('minecraft' , 'text').then(ch => {
            ch.setParent(tb);
            ch.overwritePermissions(rolemc, {
              SEND_MESSAGES: true,
              MENTION_EVERYONE: false
          })
          ch.overwritePermissions(message.guild.id, {
            SEND_MESSAGES: false,
            MENTION_EVERYONE: false
          })
          })
          message.guild.createChannel('league-of-legends' , 'text').then(ch => {
            ch.setParent(tb);
            ch.overwritePermissions(rolelol, {
              SEND_MESSAGES: true,
              MENTION_EVERYONE: false
          })
          ch.overwritePermissions(message.guild.id, {
            SEND_MESSAGES: false,
            MENTION_EVERYONE: false
          })
          })
  })
  message.guild.createChannel(`.${message.guild.name} | 🔊 .` , 'category').then(tb => {
  message.guild.createChannel('「الـقـرأن الـكـريــم | 📜」' , 'voice').then(ch => {
    ch.setParent(tb);
    ch.overwritePermissions(message.guild.id, {
      SPEAK: false//Baron#1500
})
message.guild.createChannel('「Events | 🎲 .」' , 'voice').then(ch => {
  ch.setParent(tb);
  ch.setUserLimit(50)//Baron#1500
  ch.overwritePermissions(message.guild.id, {
    SPEAK: false,
//Baron#1500
})
})
  })
})
message.guild.createChannel(`.Talking | ✋🏽 .` , 'category').then(tb => {
  message.guild.createChannel(`「${message.guild.name} | 🔊 .」` , 'voice').then(ch => {
    ch.setParent(tb);
    ch.setUserLimit(50)//Baron#1500
})
message.guild.createChannel('「Sounds | ♫ .」' , 'voice').then(ch => {
  ch.setParent(tb);
  ch.setUserLimit(50)//Baron#1500
})
  })
  message.guild.createChannel(`» Games | الالعاب .` , 'category').then(tb => {
    message.guild.createChannel(`Fortnite | فورتنآيت .` , 'voice').then(ch => {
      ch.setParent(tb);
      ch.overwritePermissions(message.guild.id, {
        CONNECT: false,
    
    })
    ch.overwritePermissions(rolefortnite, {
      CONNECT: true,
  
  })
  })//Baron#1500
  message.guild.createChannel('Minecraft | مآينكرآفت' , 'voice').then(ch => {
    ch.setParent(tb);
    ch.overwritePermissions(message.guild.id, {
      CONNECT: false,
  
  })
  ch.overwritePermissions(rolemc, {
    CONNECT: true,

})
  })//Baron#1500
  message.guild.createChannel(`Creative | كريآتف .` , 'voice').then(ch => {
    ch.setParent(tb);
    ch.overwritePermissions(message.guild.id, {
      CONNECT: false,
  
  })
  ch.overwritePermissions(rolecd, {
    CONNECT: true,//Baron#1500

})
})
message.guild.createChannel('Legends | لوول' , 'voice').then(ch => {
  ch.setParent(tb);
  ch.overwritePermissions(message.guild.id, {
    CONNECT: false,

})
ch.overwritePermissions(rolelol, {
  CONNECT: true,

})
})
    })
  message.guild.createChannel(`» DJ | الموسيقي .` , 'category').then(tb => {//Baron#1500
    message.guild.createChannel(`» Art.` , 'voice').then(ch => {
      ch.setParent(tb);
      ch.setUserLimit(15)
  })
  message.guild.createChannel(`» Fun.` , 'voice').then(ch => {
    ch.setParent(tb);
    ch.setUserLimit(15)
})
message.guild.createChannel(`» Life.` , 'voice').then(ch => {
  ch.setParent(tb);
  ch.setUserLimit(15)
})
message.guild.createChannel(`» Sing.` , 'voice').then(ch => {
ch.setParent(tb);
ch.setUserLimit(15)
})
})
  message.guild.createChannel(`» Privates | خآص .` , 'category').then(tb => {//Baron#1500
    message.guild.createChannel(`» Single.` , 'voice').then(ch => {
      ch.setParent(tb);
      ch.setUserLimit(1)
  })
  message.guild.createChannel('» Doubles.' , 'voice').then(ch => {
    ch.setParent(tb);
    ch.setUserLimit(2)
  })
  message.guild.createChannel('» Triples.' , 'voice').then(ch => {
    ch.setParent(tb);
    ch.setUserLimit(3)
  })
  message.guild.createChannel('» Forth.' , 'voice').then(ch => {
    ch.setParent(tb);
    ch.setUserLimit(4)
  })
  message.guild.createChannel('» Classic.' , 'voice').then(ch => {
    ch.setParent(tb);
    ch.setUserLimit(10)
  })
  message.guild.createChannel('» Group.' , 'voice').then(ch => {
    ch.setParent(tb);
    ch.setUserLimit(15)
  })
    }
 }
}
})

client.login(process.env.BOT_TOKEN)