const PORT = 4000;
const Myimages = require('./modules/module');

// let images = [];
// Myimages.forEach((img) =>{
//     images.push(img.download_url);
// });
// console.log(images);

const express = require("express");
const app = express();

app.set('view engine', 'ejs');

const path = require("path");

const publicDirectoryPath = path.join(__dirname, "./views/templates");

app.use(express.static(publicDirectoryPath));

app.get("", (req, res) => {
    res.render("index", {
        imgArr: Myimages
    });
});

// app.get("/img", (req, res) => {
//     res.render("img");
// });

app.listen(PORT, () => {
    console.log("Server is up on port "+ PORT);
});