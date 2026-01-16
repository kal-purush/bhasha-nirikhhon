const express=require('express');
const bodyParser=require('body-parser');
const app=express();
//const User=require("./models/user")

//Count of Users and IP address of socket
let Users=0,ip;

app.use(express.static(__dirname+"/public"));
app.set("view engine","ejs");
app.use(bodyParser.urlencoded({extended: true}));

app.get("/",(req,res) => {
    ip = req.headers['X-forwarded-for'] || 
     req.connection.remoteAddress || 
     req.socket.remoteAddress ||
     (req.connection.socket ? req.connection.socket.remoteAddress : null);
    res.render("index",{Users:Users});
});


//Server listening (Port config)
const port=process.env.PORT || 3000;
const server=app.listen(port,() => {
    console.log('Server has been started!');
});

let io=require("socket.io")(server);

io.on("connection",(socket) => {
    Users+=1;
    console.log("New User connected!");
    socket.username='Anonymous('+ip+')';
    //Username in socket stores IP address as well
    socket.on('changeUserName',(data) => {
        socket.username=data.username+'('+ip+')';
        socket.emit("changeUserName",{username:data.username});
    });
    socket.on("newMessage",(data) => {
        //Stripping IP address from socket's username to get plain username
        let startIP=socket.username.lastIndexOf('(');
        let endIP=socket.username.lastIndexOf(')');
        let usernameWithoutIP=socket.username.substring(0,startIP);
        let IPOfSender=socket.username.substring(startIP+1,endIP);
        //Check whether client is the one who sent the message
        let ISentTheMessage=false;
        if(ip==IPOfSender){
            ISentTheMessage=true;
        }
        // console.log("Hey "+ip+" "+IPOfSender+" "+socket.username+" "+startIP+" "+endIP);
        //broadcast
        io.sockets.emit("newMessage",{message:data.message,username:usernameWithoutIP,sender:ISentTheMessage});
    });
    socket.on("disconnect",function(){
        Users-=1;
        console.log('A user disconnected!');
    });
});