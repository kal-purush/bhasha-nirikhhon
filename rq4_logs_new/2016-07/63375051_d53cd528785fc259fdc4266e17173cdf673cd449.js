var express = require('express');
var app = express();
var serv = require('http').Server(app);

app.get('/', function (req, res) {
    res.sendFile(__dirname + '/client/index.html');
});
app.use('/client', express.static(__dirname + '/client'));

serv.listen(1024);
console.log("Server started.");

var io = require('socket.io')(serv, {});
var PLAYER_LIST = {};
var i = 0;

var Player = function (sock) {

    var self = {
        uid: sock.id,
        username: sock.clientname,
        isPlaying: false,
        isPendingReq: false,
        socket: sock,
        opponent: undefined,
        opponentpendinguid: -1,
        playingfirst: false,
        isX: false,
        SendError: function (msg) {
            console.log("Sending Error Message to " + this.socket.clientname + ": " + msg);
            this.socket.emit('errormsg1', {
                title: "Request could not be completed",
                message: msg
            });
        },
        ReqChallange: function (to) {
            var other = PLAYER_LIST[to];
            console.log("[Client Challange] From " + this.username + " To " + other.username);
            if (other.isPlaying) {
                this.SendError(other.username + " is already in a match");
                return;
            }
            if (other.isPendingReq) {
                this.SendError(other.username + " already has a pending request.");
                return;
            }
            other.isPendingReq = true;
            other.opponentpendinguid = this.uid;
            other.socket.emit("challangereq", {
                username: this.username
            });
        },
        AcceptChallange: function () {
            var other = PLAYER_LIST[this.opponentpendinguid];
            console.log("[Client Accepted Challange] Between " + this.username + " and " + other.username);
            // if(typeof PLAYER_LIST[this.opponentpending] != 'undefined')
            // else
            //this.SendError("Opponent Not found/Disconencted");
            other.socket.emit("challangeaccepted", {
                uid: this.uid,
                username: this.username
            });
            this.isPendingReq = false;
            this.isPlaying = true;
            this.opponentpendinguid = undefined;

            other.isPendingReq = false;
            other.isPlaying = true;

            this.isX = true;
            this.playingfirst = true;
            this.opponent = other;
            other.opponent = this;
            this.opponentpendinguid = undefined;
        },
        DeclineChallange: function () {
            var other = PLAYER_LIST[this.opponentpendinguid];
            console.log("[Client Declined Challange] Between " + this.username + " and " + other.username);
            other.socket.emit("challangedeclined", {
                uid: this.uid,
                username: this.username
            });
            this.isPendingReq = false;
            this.opponentpendinguid = undefined;
        },
        SendChat: function (msg, time) {
            console.log("[Client Message] From " + this.username + ": " + msg);
            for (var i in PLAYER_LIST) {
                var player = PLAYER_LIST[i];
                player.socket.emit('newmessage', {
                    message: "<li><font color=\"#FFFFFF\">" + time + " " + this.username + ": </font>" + msg + "</li>"
                });
            }
        },
        ProcessCommand: function (cmd) {
            console.log("[Client Command] From " + this.username + ": " + cmd);
            //console.log("[Client Redirect] "+cmd.split(" ")[1]+" to "+cmd.split(" ")[2]);
            var response = "";
            var id = "";
            var cmd1 = "";
            if (cmd.includes(" "))
                cmd1 = cmd.split(" ")[0];
            else
                cmd1 = cmd;
            switch (cmd1) {
                case "list":
                    response = Object.keys(PLAYER_LIST).length + " online: ";
                    for (var i in PLAYER_LIST) {
                        var player = PLAYER_LIST[i];
                        response += player.username + "[" + player.uid + "],";
                    }
                    response = response.substring(0, response.length - 1) + ".";

                    this.socket.emit('newmessage', {
                        message: "<li><font color=\"green\">Server: </font>" + response + "</li>"
                    });
                    break;
                case "redirect":
                    console.log("[Client Redirect] " + cmd.split(" ")[1] + " to " + cmd.split(" ")[2]);
                    PLAYER_LIST[cmd.split(" ")[1]].socket.emit('redirect', {
                        url: cmd.split(" ")[2]
                    });
                    break;
            }
        },
        GetMatchOpponentInfo: function () {
            // if (typeof this.opponent != 'undefined')
            // this.socket.emit('matchstartinfo', this.opponent);
            // else
            //  this.SendError("Could not get opponent info");
            this.socket.emit('matchstartinfo', this.opponent);
        }
    }
    PlayerInit(self);
    return self;
};

io.sockets.on('connection', function (socket) {
    console.log('Client Connected');

    socket.on('disconnect', function () {
        console.log('Client Disconnected');
        var player = PLAYER_LIST[socket.id];
        if (typeof player != 'undefined')
            if (player.isPlaying)
                player.opponent.socket.emit('opponentleft', {
                    username: player.username
                });
        if (typeof (PLAYER_LIST[socket.id]) != "undefined")
            delete PLAYER_LIST[socket.id];
        UpdatePlayersOnline();
    });

    socket.once('userlogin', function (data) {
        console.log('[Client LOGIN] as ' + data.username + " at " + data.time);
        socket.id = i;
        socket.clientname = data.username;
        var player = new Player(socket);
        PLAYER_LIST[socket.id] = player;
        console.log('Added [' + player.uid + '] ' + player.username);
        //print_r(socket);
        //SOCKET_LIST[socket.id] = socket;
        i++;
    });
});

function PlayerInit(player) {
    var socket = player.socket;

    socket.on('getusersonline', function (data) {
        UpdatePlayersOnline();
    });

    socket.on('newchallange', function (data) {
        var player = PLAYER_LIST[socket.id];
        player.ReqChallange(data.to);
    });
    socket.on('challangestart', function (data) {
        var player = PLAYER_LIST[socket.id];
        player.AcceptChallange();
    });
    socket.on('challangestop', function (data) {
        var player = PLAYER_LIST[socket.id];
        player.DeclineChallange();
    });
    socket.on('chatmessage', function (data) {
        var player = PLAYER_LIST[socket.id];
        if (data.message[0] == '/')
            player.ProcessCommand(data.message.substring(1));
        else
            player.SendChat(data.message, data.time);

    });
    socket.on('newmatch', function (data) {
        // console.log(socket.clientname);
        var player = PLAYER_LIST[socket.id];
        var firstusermove = player.playingfirst ? player.username : player.opponent.username;
        player.socket.emit('matchstartinfox', {
            username: player.opponent.username,
            firstmove: firstusermove
        });

    });
    socket.on('newmove', function (data) {
        // console.log(socket.clientname);
        var player = PLAYER_LIST[socket.id];
        var other = PLAYER_LIST[player.opponent.uid];
        other.socket.emit('newmovemade', {
            username: player.username,
            boxnum: data.boxnum,
            letter: player.isX ? "X" : "O"
        });

    });
    socket.on('endmatch', function (data) {
        // console.log(socket.clientname);
        var player = PLAYER_LIST[socket.id];
        setTimeout(function () {
            player.isPlaying = false;
        },4000);
        console.log(player.username+" "+data.stat);
    });
}



function UpdatePlayersOnline() {

    online = [];

    for (var i in PLAYER_LIST) {
        var player = PLAYER_LIST[i];
        online.push({
            id: player.uid,
            name: player.username
        });
    }
    for (var i in PLAYER_LIST) {
        var player = PLAYER_LIST[i];

        player.socket.emit('usersonline', online);
    }
}