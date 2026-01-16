require('dotenv').config();
const express = require('express')
const app = express();
const cors = require('cors')
const mongoose = require("mongoose");
const helmet = require("helmet");
const morgan = require("morgan");
const userRoute = require("./routes/users");
const authRoute = require("./routes/auth");
const postRoute = require("./routes/posts");
const conversationRoute = require("./routes/conversations");
const messageRoute = require("./routes/messages");
const path = require('path');

mongoose.connect(process.env.CLOUD_MONGO_URL,
    {useNewUrlParser:true,useUnifiedTopology:true},
    (err)=>{
        if(err){console.log(err)}
        console.log("connected to MongoDB");
    });

    // middleware
    app.use(cors())
    app.use(express.json());
    app.use(express.urlencoded({ extended: true }));
    app.use("/api/users", userRoute);
    app.use("/api/auth", authRoute);
    app.use("/api/posts", postRoute);
    app.use("/api/conversations", conversationRoute);
    app.use("/api/messages", messageRoute)
    // For frontend app to run as static file
    app.use(express.static(path.join(__dirname, "/frontend/build")));

    app.get('*', (req, res) => {
      res.sendFile(path.join(__dirname, '/frontend/build', 'index.html'));
    });

    const server = require("http").createServer(app);

    server.listen(process.env.PORT || 8900,()=>{
        console.log("backend running successfully");
        });
    

    // initilization socket.io 
    const io = require("socket.io")(server,{
        cors:{
            origin:"*",
            methods:['GET','POST']
        }
    });
    // load socket.js file and pass io object to socket.js.
    require("./socketfile")(io);