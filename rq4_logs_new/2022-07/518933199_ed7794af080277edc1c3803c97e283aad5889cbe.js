//creating express app
const cookieSession = require("cookie-session");
const express = require("express");
const passport = require("passport");
const passportSetup = require('./passport');
const cors = require("cors");
const authRoute = require("./routes/auth");
const app = express();

//2: set up the cookie session
app.use(cookieSession(
    { name: "session", keys: ["bendev"], maxAge: 24 * 60 * 60 * 100 } //one day
));

app.use(passport.initialize()); //3: initialize passport library
app.use(passport.session()); //4: use the passport session.

//5: use cors 
//it lets us send sessions through our client server requests
app.use(cors({
    origin: "http://localhost:3000",  //client server
    methods: "GET,POST,PUT,DELETE",
    credentials: true,
}));

app.use("/auth", authRoute);

//1: first step is to listen on the server
app.listen("5000", () => {
    console.log("Server is running...");
});