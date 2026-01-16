const express = require("express");
const app = express();
const path = require("path");

app.get("/api/index", (req, res) => {
  res.sendFile(path.join(__dirname, "index.html"), (err) => {
    if (err) {
      console.log(err);
    }
  });
});
app.get("/api/blitz", (req, res) => {
  res.sendFile(path.join(__dirname, "blitz.html"), (err) => {
    if (err) {
      console.log(err);
    }
  });
});
app.get("/api/node", (req, res) => {
  res.sendFile(path.join(__dirname, "node.html"), (err) => {
    if (err) {
      console.log(err);
    }
  });
});
app.get("/api/web", (req, res) => {
  res.sendFile(path.join(__dirname, "web.html"), (err) => {
    if (err) {
      console.log(err);
    }   
  });
});

app.listen(3000);

console.log(`Server working with port ...`, 3000);