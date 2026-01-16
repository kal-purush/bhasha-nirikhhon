"use strict";

const express = require("express");
const PORT = process.env.PORT || 3001;
const app = new express();

app.use(express.static("./puplic"));

app.get("/", (req, res) => {
    res.status(200).send("wonderful world");
});

app.listen(PORT, () => {
    console.log(`server is working on ${PORT}`);
});