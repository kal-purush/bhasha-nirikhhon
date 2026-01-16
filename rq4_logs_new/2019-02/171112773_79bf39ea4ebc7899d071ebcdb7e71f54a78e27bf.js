const login = require('facebook-chat-api')
const fs = require("fs");

login({email:"nampham.hl98",password:"trjng9pnamftu.07.99."},(err,api)=>{
    if(err)console.error(err);

    api.listen((err,message)=>{
        console.log(message.threadID);
        api.sendMessage(message.body,message.threadID);
    })
})


// login({email:"nampham.hl98",password:"trjng9pnamftu.07.99.",appState: JSON.parse(fs.readFileSync('appstate.json', 'utf8'))}, (err, api) => {
//     if(err) return console.error(err);

//     api.getFriendsList((err, data) => {
//         if(err) return console.error(err);
//         console.log(data);
//         console.log(data.length);
//     });
// });