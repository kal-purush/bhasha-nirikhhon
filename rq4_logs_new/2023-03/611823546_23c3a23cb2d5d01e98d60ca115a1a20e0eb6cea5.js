const express = require("express");
const expressLayouts = require("express-ejs-layouts");
const mongoose = require("mongoose");
const flash = require("connect-flash");
const session = require("express-session");
const passport = require("passport");
const fs = require("fs");
const rp = require("request-promise");
const cheerio = require("cheerio");
const crypto = require("crypto");
const hijriSafe = require("hijri-date/lib/safe");

const HijriDate = hijriSafe.default;

// Socket IO Config
const app = express();
var server = require("http").Server(app);
var io = require("socket.io")(server);

// Mongo models
const Message = require("./models/Message");
const User = require("./models/User");
const Chat = require("./models/Chat");
const { resolve } = require("path");
const { html } = require("cheerio");

// Passport config
require("./config/passport")(passport);

// DB Config
const db = require("./config/keys").MongoURI;

// Connect to Mongo
mongoose
  .connect("mongodb+srv://dfgdfgdfdfgh2:3OLVZ40yieqYe23b@cluster0.rolv8wm.mongodb.net")
  .then(() => console.log("MongoDB Connected"))
  .catch(err => console.log(err));

/*  mongoose
  .connect("mongodb://localhost:27017/myapp", { useNewUrlParser: true })
  .then(() => console.log("MongoDB Connceted"))
  .catch(err => console.log(err));  */

// EJS
app.use(expressLayouts);
app.set("view engine", "ejs");

// Public
var path = require('path');
app.use(express.static(path.resolve('./public')));

// Bodyparser
app.use(express.urlencoded({ extended: false }));

// Express session
app.use(
  session({
    secret: "secret",
    resave: true,
    saveUninitialized: true
  })
);

// Passport middleware
app.use(passport.initialize());
app.use(passport.session());

// Connect flash
app.use(flash());

// Global Vars
app.use((req, res, next) => {
  res.locals.success_msg = req.flash("success_msg");
  res.locals.error_msg = req.flash("error_msg");
  res.locals.error = req.flash("error");
  next();
});

// Routes
app.use("/", require("./routes/index"));
app.use("/users/", require("./routes/users"));

const PORT = process.env.PORT || 5000;

server.listen(PORT, console.log(`Server started on port ${PORT}`));

users = [];
tmpUsers = [];
connections = [];

let totalApproved = 0;
let totalPending = 0;
let chatOption = "undefined";
let notice = "";
let hijriDate = "";
let firstLoad = true;

const algorithm = "aes-256-cbc";
let key = crypto.randomBytes(32);
let iv = crypto.randomBytes(16);

function encrypt(text) {
  let cipher = crypto.createCipheriv(algorithm, Buffer.from(key), iv);
  let encrypted = cipher.update(text);
  encrypted = Buffer.concat([encrypted, cipher.final()]);
  return encrypted.toString('hex');
}

function decrypt(text) {
  let encryptedText = Buffer.from(text, 'hex');
  let decipher = crypto.createDecipheriv(algorithm, Buffer.from(key), iv);
  let decrypted = decipher.update(encryptedText);
  decrypted = Buffer.concat([decrypted, decipher.final()]);
  return decrypted.toString();
}

setInterval(function() {
  users = tmpUsers;
  users.sort((a, b) => (a.time > b.time) ? 1 : -1);
  io.sockets.emit("get users", users);

  tmpUsers = []; // remove all users from the array
  // Emit for active users response
  io.sockets.emit("response");
  setInterval(function() {
    io.sockets.emit("response");
  }, 60000);
  // console.log("response called");
}, 300000);

setInterval(function() {
  let currDate = hijriDate;
  //rp("https://timesprayer.com/en/hijri-date-in-bangladesh.html")
  rp("https://habibur.com")
  .then(html => {
    //currDate = cheerio(".prayertable > table > tbody > tr:nth-child(1) > td:nth-child(2)", html).text();
    currDate = cheerio(".row-fluid > div > div > div > div:nth-child(2) > table > tbody > tr > td:nth-child(1)", html).text()
    + " " + cheerio(".row-fluid > div > div > div > div:nth-child(2) > table > tbody > tr > td:nth-child(2) > a", html).text()
    + " " + cheerio(".row-fluid > div > div > div > div:nth-child(2) > table > tbody > tr > td:nth-child(3) > a", html).text();
    if(currDate !== hijriDate) {
      hijriDate = currDate;
      io.sockets.emit("date updated", chatOption);
    }  
  })
  .catch(err => console.log(err));
  //currDate = new HijriDate();
}, 300000);

// Connection
io.sockets.on("connection", function(socket) {
  connections.push(socket);
  updateUsernames(); // To update users list on startup
  // Get chats from mongo
  Message.find()
    .sort({ _id: -1 })
    .limit(50)
    .exec(function(err, output) {
      if (err) {
        throw err;
      }
      // console.log(output);

      /*var hw = encrypt("Some serious stuff");
      console.log(hw);
      console.log(decrypt(hw));*/

      let unrecognized = false;
      if(firstLoad) {
        firstLoad = false;
        if(output.length > 0) {
          io.sockets.emit("chat deleted");

          unrecognized = true;
          Message.collection.drop();

          const path = "./routes/chatlog.txt";

          fs.unlink(path, err => {
            if (err) {
              // console.error(err)
              return;
            }
          });
        }
    
      }

      if(unrecognized) {
        socket.emit("output", []);
      }
      else {
        for (let i = 0; i < output.length; i++) {
          output[i].message = decrypt(output[i].message);
        }

        // Emit the messages
        socket.emit("output", output);
      }
    });

  //socket.on("load chat", function() {
  if (chatOption === "undefined") {
    Chat.findOne({ key: "chat" }).then(chat => {
      if (chat) {
        chatOption = chat.isEnabled;
      } else {
        chatOption = true;
        let newChat = new Chat({
          key: "chat",
          isEnabled: true
        });
        newChat
        .save()
        .then(chat => {
          console.log("Chat Enabled: " + chat.isEnabled);
        })
        .catch(err => console.log(err));
      }
    })
    .catch(err => console.log(err));
  }
  //rp("https://timesprayer.com/en/hijri-date-in-bangladesh.html")
  rp("https://habibur.com")
  .then(html => {
    //hijriDate = cheerio(".prayertable > table > tbody > tr:nth-child(1) > td:nth-child(2)", html).text();
    hijriDate = cheerio(".row-fluid > div > div > div > div:nth-child(2) > table > tbody > tr > td:nth-child(1)", html).text()
    + " " + cheerio(".row-fluid > div > div > div > div:nth-child(2) > table > tbody > tr > td:nth-child(2) > a", html).text()
    + " " + cheerio(".row-fluid > div > div > div > div:nth-child(2) > table > tbody > tr > td:nth-child(3) > a", html).text();
    socket.emit("l-chat", chatOption, notice, hijriDate);
    socket.emit("l-toggle", chatOption);  
  })
  .catch(err => console.log(err));
  //hijriDate = new HijriDate();
  //});

  // Delete Notice
  socket.on("upd", function(data) {

    notice = data;
    // Send notice change event
    io.sockets.emit("notice updated", notice);

  });

  // Toggle Chat Mode
  socket.on("toggle now", function() {
    if (chatOption) {
      chatOption = false;
    } else {
      chatOption = true;
    }

    // Send toggle event
    io.sockets.emit("toggled", chatOption);

    Chat.updateOne({ key: "chat" }, { $set: { isEnabled: chatOption } }).exec(function(err) {
      if (err) {
        throw err;
      }

    });
  });

  // Download chat
  socket.on("dwn", function() {
    // console.log('ok');
    Message.find().exec(function(err, output) {
      if (err) {
        throw err;
      }
      var allmsg = "";
      var l = output.length;
      for (i = 0; i < l; i++) {
        allmsg += output[i].name + "(" + output[i].time + ") : " + decrypt(output[i].message) + "\r\n";
      }
      // console.log(output.length);
      // write to a new file named 2pac.txt
      fs.writeFile("./routes/chatlog.txt", allmsg, err => {
        // throws an error, you could also catch it here
        if (err) throw err;

        // success case, the file was saved
        // console.log('saved!');
      });
    });
  });

  // Delete all chats
  socket.on("dlt", function() {
    io.sockets.emit("chat deleted");

    Message.find().exec(function(err, output) {
      if (err) {
        throw err;
      }
      if(output.length > 0) {
        Message.collection.drop();
      }
    });

    key = crypto.randomBytes(32);
    iv = crypto.randomBytes(16);

    const path = "./routes/chatlog.txt";

    fs.unlink(path, err => {
      if (err) {
        // console.error(err)
        return;
      }
    });
  });

  // Load approved users
  socket.on("approved", function() {
    User.find({ isApproved: true })
    .sort({ _id: -1 })
    .limit(50)
    .exec(function(err, output) {
      if (err) {
        throw err;
      }

      totalApproved += output.length;
      // Send load more messages
      socket.emit("approved users", output);
    });
  });  

  // Load pending users
  socket.on("pending", function() {
    User.find({ isApproved: false })
    .sort({ _id: -1 })
    .limit(50)
    .exec(function(err, output) {
      if (err) {
        throw err;
      }

      totalPending += output.length;
      // Send load more messages
      socket.emit("pending users", output);
    });
  });  
  
  // Load more chats
  socket.on("loadmore", function(data) {
    Message.find().exec(function(err, output) {
      if (err) {
        throw err;
      }
      output.reverse();
      for (i = 0; i < data; i++) {
        output.shift();
      }

      for (let i = 0; i < output.length; i++) {
        output[i].message = decrypt(output[i].message);
      }
      // Send load more messages
      socket.emit("lm-meesages", output.slice(0, 15));
    });
  });

  // Load more approved users
  socket.on("loaduser", function(data) {
    User.find({ isApproved: true }).exec(function(err, output) {
      if (err) {
        throw err;
      }
      output.reverse();
      for (i = 0; i < data; i++) {
        output.shift();
      }

      totalApproved += 15;
      // Send load more messages
      socket.emit("lm-users", output.slice(0, 15));
    });
  });

  // Load more pending users
  socket.on("loadpending", function(data) {
    User.find({ isApproved: false }).exec(function(err, output) {
      if (err) {
        throw err;
      }
      output.reverse();
      for (i = 0; i < data; i++) {
        output.shift();
      }

      totalPending += 15;
      // Send load more messages
      socket.emit("lm-pending", output.slice(0, 15));
    });
  });
  
  // Approve user
  socket.on("approve now", function(data, user) {
    totalApproved++;
    const newData = {
      name: data,
      isApproved: true,
      isAdmin: false,
      approvedBy: user
    }

    // Send load more messages
    io.sockets.emit("new approved", newData, totalApproved);
    io.sockets.emit("remove pending", data);
    
    User.updateOne({ name: data }, { $set: { isApproved: true, approvedBy: user } }).exec(function(err) {
      if (err) {
        throw err;
      }

    });
  });

  // Delete user
  socket.on("delete now", function(data, flag) {
    // Send load more messages
    if(flag === 1) {
      io.sockets.emit("remove pending", data);
    } else {
      io.sockets.emit("remove approved", data);
    }

    User.deleteOne({ name: data }).exec(function(err) {
      if (err) {
        throw err;
      }

    });
  });

  // Change user role
  socket.on("change role", function(data) {
    User.findOne({ name: data }).exec(function(err, result) {
      if (err) {
        throw err;
      }

      let role = false;
      if(result.isAdmin === false) {
        role = true;
      }

      const newData = {
        name: data,
        isAdmin: role
      }
      // Send load more messages
      io.sockets.emit("role changed", newData);

      User.updateOne({ name: data }, { $set: { isAdmin: role } }).exec(function(err) {
        if (err) {
          throw err;
        }
  
      });
    });
  });

  // Delete Single Message 
  socket.on("delete message", function(data) {
    User.findOne({ name: data.username }).exec(function(err, result) {
      if (err) {
        throw err;
      }
      Message.deleteOne({ _id: data.message_id, name: data.username}).exec(function(err, result) {
        if (err) {
          throw err;
        }
      });
      // Send message deleted signal 
      io.sockets.emit("message deleted", {_id: data.message_id});
    });
  });

  // New user
  socket.on("username", function(data, callback) {
    ut = { username: data.user, time: data.time };
    nope = true;
    tmpNope = true;

    for (var x = 0; x < users.length; x++) {
      if (users[x].username === data.user) {
        nope = false;
      }
    }

    for (var x = 0; x < tmpUsers.length; x++) {
      if (tmpUsers[x].username === data.user) {
        tmpNope = false;
      }
    }

    if (nope) {
      users.push(ut);
      if (tmpNope) {
        tmpUsers.push(ut);
      }
      // New user emitting
      io.sockets.emit("nUser", data);
    }

  });

  // User Disconnect
  /*socket.on("disconnect", function() {
    setTimeout(function() {
      tmpUsers = users;
      users = []; // remove all users from the array
      // Emit for active users response
      io.sockets.emit("response");
      // console.log("response called");  
    }, 3000);
  });*/

  // After 1 minute users will be refreshed
  /*let urf = function() {
    tmpUsers = users;
    users = []; // remove all users from the array
    // Emit for Active users response
    io.sockets.emit("response");
  };
  setInterval(urf, 10000);*/

  // Active users response
  socket.on("actRes", data => {
    ut = { username: data.user, time: data.time };
    for (var x = 0; x < users.length; x++) {
      if (users[x].username === data.user) {
        ut = { username: data.user, time: users[x].time };
      }
    }

    nope = true;
    for (var x = 0; x < tmpUsers.length; x++) {
      if (tmpUsers[x].username === data.user) {
        nope = false;
      }
    }

    if (nope) {
      // Pussing active users to users array
      tmpUsers.push(ut);
    }

    // Emit to mark Active users
    //io.sockets.emit("actM", data.user);
    // console.log("actM called");

    /*let fxn = function() {
      // Emit to remove InActive users after 3 sec
      //io.sockets.emit("inAct");
      users = tmpUsers;
      io.sockets.emit("get users", users);
    };
    setTimeout(fxn, 30000);*/
  });

  // Logout
  socket.on("lg", function(data) {
    // console.log('called');
    for (var i = 0; i < users.length; i++) {
      if (users[i].username == data) {
        users.splice(i, 1);
        break;
      }
    }

    for (var i = 0; i < tmpUsers.length; i++) {
      if (tmpUsers[i].username == data) {
        tmpUsers.splice(i, 1);
        break;
      }
    }

    io.sockets.emit("lgUser", data);
  });

  // Send Message
  socket.on("send message", function(data) {
    const newMessage = new Message({
      name: data.user,
      message: encrypt(data.msg),
      time: data.time
    });

    // Save user
    newMessage
      .save()
      .then(message => {
        io.sockets.emit("new message", {
          user: data.user,
          msg: data.msg,
          time: data.time,
          _id: message._id
        });
      })
      .catch(err => console.log(err));
  });

  function updateUsernames() {
    // Emit users list
    socket.emit("get users", users);
  }
});

module.exports.func = (user) => {
  io.sockets.emit("registered", user, totalPending);
};