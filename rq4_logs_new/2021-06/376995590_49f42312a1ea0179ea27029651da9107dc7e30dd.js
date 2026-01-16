const express = require("express");
const mongoose = require("mongoose");
const cors = require("cors");
const app = express();

mongoose.connect("mongodb://localhost/forms_db", {
    useNewUrlParser: true,
    useUnifiedTopology: true,
    useCreateIndex: true,
  });

const heroRoutes = require('./routes/hero');


app.use(cors());
app.use((req, res, next) => {
    res.header('Access-Control-Allow-Origin', '*')
    next();
})
app.use(express.json()); // ALWAYS define your middleware before your routes
app.use(express.urlencoded({ extended: true }));


app.use("/hero", heroRoutes.routes);

app.listen(3000);