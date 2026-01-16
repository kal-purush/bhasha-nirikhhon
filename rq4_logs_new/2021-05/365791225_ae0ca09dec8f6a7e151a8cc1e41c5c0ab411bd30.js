const nodemailer = require("nodemailer");
const express = require("express")
const dotenv = require("dotenv")
const app = express()

app.use(express.static("public"))
app.set("view engine", "ejs")
app.use(express.urlencoded({ extended: true }));

dotenv.config()

const transporter =  nodemailer.createTransport({
  host: 'smtp.gmail.com',
  port: 465,
  secure: true, // use SSL
  auth: {
      user: process.env.USER,
      pass: process.env.PASS
  }
});

app.get("/", (req, res) => {
    res.render("index")
})

app.post("/send", (req, res) => {
  const email = req.body.email
  var message = {
    from: process.env.USER,
    to: email,
    subject: "Success",
    html: "<p><strong>Successfully sent the email! Now you understand the SMTP and mail services. If you love it, please rate the <a href = \"link\">project</a></strong></p>"
  };

  transporter.sendMail(message, function(err, data) {
    if (err) {
      console.log(err);
    } else {
      console.log(info);
    }
  });

  res.redirect("back")
})

app.listen(8080, () => console.log("Server started.."))