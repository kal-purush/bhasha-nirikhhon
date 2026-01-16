const { resolve } = require("path");
const express = require("express"); //importe la librairie express
const nodemailer = require("nodemailer"); //importe la librairie nodemailer
const bodyParser = require("body-parser"); //importe la librairie body-parser
const Config = require("./Config.js"); //importe mon fichier Config.js

//-----------------------EXPRESS-LOCALHOST---------------------------//
const app = express(); // Créer l'application express
app.use(express.static(resolve(__dirname, "public")));
app.use(bodyParser.urlencoded({ extended: true }));
app.listen(8000, () => { // Ecoute sur le port 8000
    console.log("Server a démarer dans http://localhost:8000"); // Renvoi le message Server a démarer dans http://localhost:8000
});

//--------------------ROUTE-ET-ENVOI-DE-MAIL------------------------//
let transporter = nodemailer.createTransport({
    host: "smtp.gmail.com",
    auth: {
        type: "login", // default
        user: Config.mail,     //| Je recupére le Config.mail de mon fichier Config.js
        pass: Config.mdpMail,  //| Je recupére le Config.mdpMail de mon fichier Config.js
    },
});

app.post("/contact", async (req, res) => {
    let message = "";
    let mailOptions = {
        from: req.body.email,
        to: "buirechristophe@gmail.com",
        subject: `demande de contact de ${req.body.fisrtname} ${req.body.email}`,
        text: req.body.message,
    };

    transporter.sendMail(mailOptions, (err) => {
        if (err) {
            message = "Votre message n'est pas transmis ! Mail: Buirechristophe@gmail.com";
            console.log(err);
            res.render("page-pf/contact.html.twig", { message });
        } else {
            message = "Votre message est transmis !";
            res.render("page-pf/contact.html.twig", { message });
        }
    });
});

//------------------------------ROUTE-------------------------------//
app.get("/contact", async (req, res) => {
    res.render("page-pf/contact.html.twig");
});

app.get("/", async (req, res) => {
    res.render("page-pf/index.html.twig");
});

app.get("/cv", async (req, res) => {
    res.render("page-pf/cv.html.twig");
});

app.get("/projets", async (req, res) => {
    res.render("page-pf/projets.html.twig");
});
