const express = require("express");
const app = express();
const path = require("path");

const port = process.env.PORT || 5001;
const host = "localhost";

app.set("view engine", "ejs");
app.set("views", path.join(__dirname + "/views"));

//Public folder is being convert static
app.use(express.static(path.join(__dirname, "public")));

app.get("/", (req, res) => {
    res.render("home");
})

app.get("/register", (req, res) => {
    res.render("register");
})

app.listen(port, (req, res) => {
    console.log(`I'm listening http://${host}:${port}`)
})