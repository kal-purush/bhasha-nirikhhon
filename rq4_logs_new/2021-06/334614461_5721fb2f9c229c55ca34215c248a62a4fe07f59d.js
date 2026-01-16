const bp = require("body-parser");
const express = require("express");
const requests = require("requests");
const port = process.env.port || 3000;
const app = express();

app.set("view engine", "ejs");
app.use(express.static("public"));
app.use(bp.urlencoded({ extended: true }));

app.get("/", (req, res) => {
    res.render("index", { title: "NewsLetter" })
})
app.post("/", (req, res) => {
    var fn = req.body.fname;
    var sn = req.body.lname;
    var em = req.body.user_email;
    console.log(fn, sn, em);


})

app.listen(port, () => {
    console.log(`You server is running on port number:${port}`)
})


// 8f6eac23bef2f7d576e2c04c03d7863f-us6