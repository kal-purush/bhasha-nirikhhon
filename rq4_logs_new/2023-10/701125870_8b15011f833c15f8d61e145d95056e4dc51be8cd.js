const express = require("express");
const app = express();
const HTTP_PORT = process.env.PORT || 8080;
const path = require("path");
const { engine } = require('express-handlebars');

app.use(express.urlencoded({ extended: true }))
app.engine('.hbs', engine({ extname: '.hbs' }));
app.set("views", "./views");
app.set('view engine', '.hbs');


// ----------endpoints--------
app.get("/", (req, res) => {
    console.log(`server started!!!`)
    res.render("index", {
        layout: false
    })
    // res.send(`server endpoint working `);
})


// ----------endpoints--------


const onHttpStart = () => {
    console.log(`The web server has started at http://localhost:${HTTP_PORT}`);
    console.log("Press CTRL+C to stop the server.");
};
app.listen(HTTP_PORT, onHttpStart);